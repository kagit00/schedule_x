package com.shedule.x.service;

import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingContext;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.models.*;
import com.shedule.x.processors.EdgePersistence;
import com.shedule.x.processors.GraphPreProcessor;
import com.shedule.x.processors.PerfectMatchSaver;
import com.shedule.x.processors.matcher.strategies.MatchingStrategy;
import com.shedule.x.processors.matcher.strategies.decider.MatchingStrategySelector;
import com.shedule.x.repo.LastRunPerfectMatchesRepository;
import com.shedule.x.repo.MatchingConfigurationRepository;
import com.shedule.x.repo.NodeRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.*;


import lombok.RequiredArgsConstructor;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@Service
@RequiredArgsConstructor
public class PerfectMatchServiceImpl implements PerfectMatchService {
    private final EdgePersistence edgePersistence;
    private final PerfectMatchSaver perfectMatchSaver;
    private final MatchingStrategySelector strategySelector;
    private final MatchingConfigurationRepository matchingConfigurationRepository;
    private final MeterRegistry meterRegistry;
    private final GraphPreProcessor graphPreProcessor;
    private final LastRunPerfectMatchesRepository lastRunRepository;
    private final NodeRepository nodeRepo;

    @Value("${matching.topk.count:100}")
    private int topK;

    private final ExecutorService cpuExecutor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors())
    );

    @Override
    public CompletableFuture<Void> processAndSaveMatches(MatchingRequest request) {
        UUID groupId = request.getGroupId();
        UUID domainId = request.getDomainId();
        String cycleId = request.getProcessingCycleId();

        Timer.Sample timer = Timer.start(meterRegistry);

        return CompletableFuture
                .supplyAsync(() -> buildMatchingContext(groupId, domainId, request), cpuExecutor)
                .thenCompose(context -> {
                    if (context == null) {
                        log.error("MatchingContext is null for groupId={}, domainId={}", groupId, domainId);
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("MatchingContext is null"));
                    }

                    // Safe null handling for lastRun
                    LastRunPerfectMatches lastRun = lastRunRepository
                            .findByDomainIdAndGroupId(domainId, groupId)
                            .orElseGet(() -> createDefaultLastRun(domainId, groupId));

                    // Safe unboxing - handle null Long
                    long lastSuccessfulNodeCount = Optional.of(lastRun.getNodeCount())
                            .orElse(0L);

                    long currentNodeCount = context.getSizeOfNodes();

                    log.info("Perfect match check: groupId={}, domainId={}, cycleId={}, currentNodes={}, lastProcessedNodes={}",
                            groupId, domainId, cycleId, currentNodeCount, lastSuccessfulNodeCount);

                    if (currentNodeCount == 0) {
                        log.warn("No nodes found in context for groupId={}, domainId={}, cycleId={}",
                                groupId, domainId, cycleId);
                        return CompletableFuture.completedFuture(null);
                    }

                    if (currentNodeCount <= lastSuccessfulNodeCount) {
                        log.info("No new nodes to process for perfect match (current={}, last={}), skipping groupId={}, domainId={}, cycleId={}",
                                currentNodeCount, lastSuccessfulNodeCount, groupId, domainId, cycleId);
                        return CompletableFuture.completedFuture(null);
                    }

                    log.info("Processing {} new nodes for perfect match, groupId={}, domainId={}, cycleId={}",
                            currentNodeCount - lastSuccessfulNodeCount, groupId, domainId, cycleId);

                    return runPerfectMatchFromLmdb(context, groupId, domainId, cycleId);
                })
                .whenComplete((v, ex) -> {
                    timer.stop(meterRegistry.timer("perfect_match_duration_seconds",
                            "groupId", groupId.toString(),
                            "domainId", domainId.toString(),
                            "cycleId", cycleId,
                            "status", ex == null ? "success" : "failure"));
                    if (ex != null) {
                        log.error("PerfectMatch failed for groupId={}, domainId={}, cycleId={}",
                                groupId, domainId, cycleId, ex);
                    } else {
                        log.info("PerfectMatch completed successfully for groupId={}, domainId={}, cycleId={}",
                                groupId, domainId, cycleId);
                    }
                });
    }

    private LastRunPerfectMatches createDefaultLastRun(UUID domainId, UUID groupId) {
        LastRunPerfectMatches lastRun = new LastRunPerfectMatches();
        lastRun.setDomainId(domainId);
        lastRun.setGroupId(groupId);
        lastRun.setNodeCount(0L);
        lastRun.setStatus(JobStatus.PENDING.name());
        lastRun.setRunDate(LocalDateTime.now());
        return lastRun;
    }

    private MatchingContext buildMatchingContext(UUID groupId, UUID domainId, MatchingRequest request) {
        try {
            var config = matchingConfigurationRepository
                    .findByGroupIdAndDomainId(groupId, domainId)
                    .orElseThrow(() -> new IllegalStateException(
                            "Missing MatchingConfiguration for groupId=" + groupId + ", domainId=" + domainId));

            MatchType matchType = graphPreProcessor.determineMatchTypeFromExistingData(groupId, domainId);
            long totalProcessedNodes = nodeRepo.countByDomainIdAndGroupIdAndProcessedTrue(domainId, groupId);

            if (totalProcessedNodes == 0) {
                log.warn("Total processed node count is 0 for groupId={}, domainId={}. Cannot build context.",
                        groupId, domainId);
            }

            return GraphRequestFactory.buildMatchingContext(
                    groupId, domainId, (int) totalProcessedNodes, matchType,
                    config.getGroup().isCostBased(),
                    config.getGroup().getIndustry(),
                    request
            );
        } catch (Exception e) {
            log.error("Error building matching context for groupId={}, domainId={}", groupId, domainId, e);
            throw e;
        }
    }

    private CompletableFuture<Void> runPerfectMatchFromLmdb(
            MatchingContext context,
            UUID groupId,
            UUID domainId,
            String cycleId) {

        MatchingStrategy strategy = strategySelector.select(context, groupId);
        if (strategy == null) {
            log.error("MatchingStrategy is null for groupId={}, domainId={}", groupId, domainId);
            return CompletableFuture.failedFuture(
                    new IllegalStateException("MatchingStrategy is null for groupId=" + groupId));
        }

        final int BATCH_SIZE = 25_000;
        List<EdgeDTO> buffer = new ArrayList<>(BATCH_SIZE);
        List<CompletableFuture<Void>> futures = new CopyOnWriteArrayList<>();
        AtomicLong edgeCount = new AtomicLong(0);
        AtomicLong batchCount = new AtomicLong(0);

        return CompletableFuture.runAsync(() -> {
            try (var edgeStream = edgePersistence.streamEdges(domainId, groupId)) {
                log.info("Starting LMDB edge streaming for PerfectMatch | groupId={}, domainId={}, cycleId={}",
                        groupId, domainId, cycleId);

                edgeStream.forEach(edge -> {
                    edgeCount.incrementAndGet();
                    buffer.add(edge);

                    if (buffer.size() >= BATCH_SIZE) {
                        List<EdgeDTO> batch = new ArrayList<>(buffer);
                        buffer.clear();
                        long batchNum = batchCount.incrementAndGet();

                        CompletableFuture<Void> task = CompletableFuture.runAsync(
                                () -> processEdgeBatch(batch, strategy, groupId, domainId, cycleId, batchNum),
                                cpuExecutor
                        ).exceptionally(ex -> {
                            log.error("Error processing edge batch {} for groupId={}, domainId={}, cycleId={}",
                                    batchNum, groupId, domainId, cycleId, ex);
                            meterRegistry.counter("perfect_match_batch_errors",
                                    "groupId", groupId.toString(),
                                    "domainId", domainId.toString(),
                                    "cycleId", cycleId).increment();
                            return null;
                        });
                        futures.add(task);
                    }
                });

                // Final batch
                if (!buffer.isEmpty()) {
                    List<EdgeDTO> finalBatch = new ArrayList<>(buffer);
                    long finalBatchNum = batchCount.incrementAndGet();

                    CompletableFuture<Void> finalTask = CompletableFuture.runAsync(
                            () -> processEdgeBatch(finalBatch, strategy, groupId, domainId, cycleId, finalBatchNum),
                            cpuExecutor
                    ).exceptionally(ex -> {
                        log.error("Error processing final edge batch for groupId={}, domainId={}, cycleId={}",
                                groupId, domainId, cycleId, ex);
                        meterRegistry.counter("perfect_match_batch_errors",
                                "groupId", groupId.toString(),
                                "domainId", domainId.toString(),
                                "cycleId", cycleId).increment();
                        return null;
                    });
                    futures.add(finalTask);
                }

                log.info("Finished reading LMDB edges. Total edges: {}, Batches: {} | groupId={}, domainId={}, cycleId={}",
                        edgeCount.get(), futures.size(), groupId, domainId, cycleId);

            } catch (Exception e) {
                log.error("Error during LMDB streaming for groupId={}, domainId={}, cycleId={}",
                        groupId, domainId, cycleId, e);
                throw new CompletionException(e);
            }
        }, cpuExecutor).thenCompose(v -> {
            if (futures.isEmpty()) {
                log.warn("No batches created for groupId={}, domainId={}, cycleId={}",
                        groupId, domainId, cycleId);
                return CompletableFuture.completedFuture(null);
            }

            log.info("Waiting for {} batches to complete for groupId={}, domainId={}, cycleId={}",
                    futures.size(), groupId, domainId, cycleId);

            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Error in batch processing for groupId={}, domainId={}, cycleId={}",
                                    groupId, domainId, cycleId, ex);
                        } else {
                            log.info("All batches completed successfully for groupId={}, domainId={}, cycleId={}",
                                    groupId, domainId, cycleId);
                        }
                    });
        });
    }

    private void processEdgeBatch(
            List<EdgeDTO> edges,
            MatchingStrategy strategy,
            UUID groupId,
            UUID domainId,
            String cycleId,
            long batchNum) {

        if (edges == null || edges.isEmpty()) {
            log.debug("Empty edge batch {} for groupId={}, cycleId={}", batchNum, groupId, cycleId);
            return;
        }

        log.debug("Processing batch {} with {} edges for groupId={}, cycleId={}",
                batchNum, edges.size(), groupId, cycleId);

        Map<String, PriorityQueue<GraphRecords.PotentialMatch>> adjacency = new HashMap<>();

        try {
            for (EdgeDTO e : edges) {
                String from = e.getFromNodeHash();
                String to = e.getToNodeHash();
                float score = e.getScore();

                // Forward edge
                adjacency.computeIfAbsent(from, k -> newPriorityQueue())
                        .offer(new GraphRecords.PotentialMatch(from, to, score, groupId, domainId));

                // Reverse edge (undirected graph)
                adjacency.computeIfAbsent(to, k -> newPriorityQueue())
                        .offer(new GraphRecords.PotentialMatch(to, from, score, groupId, domainId));
            }

            // Trim to top-K per node
            adjacency.forEach((node, pq) -> {
                while (pq.size() > topK) pq.poll();
            });

            // Run the actual matching algorithm
            Map<String, List<MatchResult>> results = strategy.match(
                    adjacency.values().stream()
                            .flatMap(Collection::stream)
                            .toList(),
                    groupId,
                    domainId
            );

            if (results == null || results.isEmpty()) {
                log.debug("No match results for batch {}, groupId={}, cycleId={}",
                        batchNum, groupId, cycleId);
                return;
            }

            // Convert to JPA entities
            List<PerfectMatchEntity> perfectMatches = results.entrySet().stream()
                    .flatMap(entry -> entry.getValue().stream()
                            .map(mr -> PerfectMatchEntity.builder()
                                    .groupId(groupId)
                                    .domainId(domainId)
                                    .referenceId(entry.getKey())
                                    .matchedReferenceId(mr.getPartnerId())
                                    .compatibilityScore(mr.getScore())
                                    .matchedAt(LocalDateTime.now())
                                    .build()))
                    .toList();

            // Save async (non-blocking)
            if (!perfectMatches.isEmpty()) {
                perfectMatchSaver.saveMatchesAsync(perfectMatches, groupId, domainId, cycleId)
                        .thenAccept(v -> {
                            meterRegistry.counter("perfect_matches_saved_total",
                                            "groupId", groupId.toString(),
                                            "domainId", domainId.toString(),
                                            "cycleId", cycleId)
                                    .increment(perfectMatches.size());
                            log.debug("Saved {} perfect matches from batch {} for groupId={}, cycleId={}",
                                    perfectMatches.size(), batchNum, groupId, cycleId);
                        })
                        .exceptionally(ex -> {
                            log.error("Failed to save {} perfect matches from batch {} for groupId={}, domainId={}, cycleId={}",
                                    perfectMatches.size(), batchNum, groupId, domainId, cycleId, ex);
                            meterRegistry.counter("perfect_matches_save_errors",
                                            "groupId", groupId.toString(),
                                            "domainId", domainId.toString(),
                                            "cycleId", cycleId)
                                    .increment();
                            return null;
                        });
            }

        } catch (Exception e) {
            log.error("Error processing batch {} for groupId={}, domainId={}, cycleId={}",
                    batchNum, groupId, domainId, cycleId, e);
            throw e;
        }
    }

    private PriorityQueue<GraphRecords.PotentialMatch> newPriorityQueue() {
        return new PriorityQueue<>(topK + 1,
                Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore).reversed());
    }
}