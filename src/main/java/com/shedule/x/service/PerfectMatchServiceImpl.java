package com.shedule.x.service;

import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingContext;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.models.*;
import com.shedule.x.processors.EdgePersistence;
import com.shedule.x.processors.GraphPreProcessor;
import com.shedule.x.processors.PerfectMatchSaver;
import com.shedule.x.processors.matcher.strategies.MatchingStrategy;
import com.shedule.x.processors.matcher.strategies.decider.MatchingStrategySelector;
import com.shedule.x.repo.MatchingConfigurationRepository;
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

    @Value("${matching.topk.count:100}")
    private int topK;

    // Parallelism tuned for your 2GB container
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
                .thenCompose(context -> runPerfectMatchFromLmdb(context, groupId, domainId, cycleId))
                .whenComplete((v, ex) -> {
                    timer.stop(meterRegistry.timer("perfect_match_duration_seconds",
                            "groupId", groupId.toString(),
                            "domainId", domainId.toString(),
                            "cycleId", cycleId));
                    if (ex != null) {
                        log.error("PerfectMatch failed for groupId={}, domainId={}", groupId, domainId, ex);
                    } else {
                        log.info("PerfectMatch completed successfully for groupId={}, domainId={}", groupId, domainId);
                    }
                });
    }

    private MatchingContext buildMatchingContext(UUID groupId, UUID domainId, MatchingRequest request) {
        var config = matchingConfigurationRepository
                .findByGroupIdAndDomainId(groupId, domainId)
                .orElseThrow(() -> new IllegalStateException("Missing MatchingConfiguration"));

        MatchType matchType = graphPreProcessor.determineMatchTypeFromExistingData(groupId, domainId);

        return GraphRequestFactory.buildMatchingContext(
                groupId, domainId, 0, matchType,
                config.getGroup().isCostBased(),
                config.getGroup().getIndustry(),
                request
        );
    }

    private CompletableFuture<Void> runPerfectMatchFromLmdb(
            MatchingContext context,
            UUID groupId,
            UUID domainId,
            String cycleId) {

        MatchingStrategy strategy = strategySelector.select(context, groupId);

        final int BATCH_SIZE = 25_000;
        List<EdgeDTO> buffer = new ArrayList<>(BATCH_SIZE);
        List<CompletableFuture<Void>> futures = new CopyOnWriteArrayList<>();

        return CompletableFuture.runAsync(() -> {
            try (var edgeStream = edgePersistence.streamEdges(domainId, groupId)) {
                log.info("Starting LMDB edge streaming for PerfectMatch | groupId={}", groupId);

                edgeStream.forEach(edge -> {
                    buffer.add(edge);

                    if (buffer.size() >= BATCH_SIZE) {
                        List<EdgeDTO> batch = new ArrayList<>(buffer);
                        buffer.clear();

                        CompletableFuture<Void> task = CompletableFuture.runAsync(
                                () -> processEdgeBatch(batch, strategy, groupId, domainId, cycleId),
                                cpuExecutor
                        );
                        futures.add(task);
                    }
                });

                // Final batch
                if (!buffer.isEmpty()) {
                    List<EdgeDTO> finalBatch = new ArrayList<>(buffer);
                    CompletableFuture<Void> finalTask = CompletableFuture.runAsync(
                            () -> processEdgeBatch(finalBatch, strategy, groupId, domainId, cycleId),
                            cpuExecutor
                    );
                    futures.add(finalTask);
                }

                log.info("Finished reading LMDB edges. Submitted {} batches | groupId={}", futures.size(), groupId);

            } catch (Exception e) {
                log.error("Error during LMDB streaming for groupId={}", groupId, e);
                throw new CompletionException(e);
            }
        }).thenCompose(v -> {
            // Wait for all batches to complete
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        });
    }

    private void processEdgeBatch(
            List<EdgeDTO> edges,
            MatchingStrategy strategy,
            UUID groupId,
            UUID domainId,
            String cycleId) {

        // Tiny in-memory adjacency map — only 25k edges → ~30-50MB max
        Map<String, PriorityQueue<GraphRecords.PotentialMatch>> adjacency = new HashMap<>();

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
                    .thenAccept(v -> meterRegistry.counter("perfect_matches_saved_total",
                            "groupId", groupId.toString()).increment(perfectMatches.size()))
                    .exceptionally(ex -> {
                        log.error("Failed to save {} perfect matches for groupId={}", perfectMatches.size(), groupId, ex);
                        return null;
                    });
        }
    }

    private PriorityQueue<GraphRecords.PotentialMatch> newPriorityQueue() {
        return new PriorityQueue<>(topK + 1,
                Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore).reversed());
    }
}