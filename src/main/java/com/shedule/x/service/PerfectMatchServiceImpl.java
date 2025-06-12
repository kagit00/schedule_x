package com.shedule.x.service;

import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.models.*;
import com.shedule.x.processors.GraphPreProcessor;
import com.shedule.x.processors.PerfectMatchSaver;
import com.shedule.x.processors.matcher.strategies.MatchingStrategy;
import com.shedule.x.processors.matcher.strategies.decider.MatchingStrategySelector;
import com.shedule.x.repo.MatchingConfigurationRepository;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.*;
import static com.shedule.x.utils.db.BatchUtils.partition;

@Slf4j
@Service
public class PerfectMatchServiceImpl implements PerfectMatchService {

    private final PotentialMatchStreamingService potentialMatchStreamingService;
    private final PerfectMatchSaver perfectMatchSaver;
    private final MatchingStrategySelector strategySelector;
    private final MatchingConfigurationRepository matchingConfigurationRepository;
    private final MeterRegistry meterRegistry;
    private final ExecutorService ioExecutor;
    private final ExecutorService cpuExecutor;
    private final GraphPreProcessor graphPreProcessor;

    @Value("${matching.topk.count:100}")
    private int maxMatchesPerNode;
    @Value("${matching.max.memory.mb:1024}")
    private long maxMemoryMb;

    private static final int BASE_SUB_BATCH_SIZE = 500;
    private static final double MEMORY_THRESHOLD_RATIO = 0.8;
    private static final int BATCH_SIZE_FROM_CURSOR = 5000;
    private static final int MAX_NODES_PER_BATCH = 10000;
    private static final int PAGE_PROCESSING_TIMEOUT_SECONDS = 300;
    private static final int SAVE_MATCHES_TIMEOUT_SECONDS = 600;
    private static final int NODES_PER_PROCESSING_BATCH = 100;

    private final AtomicInteger currentAdjacencyMapSize = new AtomicInteger(0);
    private final Semaphore cpuTaskSemaphore;
    private final AtomicBoolean memoryExceeded = new AtomicBoolean(false);

    public PerfectMatchServiceImpl(
            PerfectMatchSaver perfectMatchSaver,
            MatchingStrategySelector strategySelector,
            MatchingConfigurationRepository matchingConfigurationRepository,
            MeterRegistry meterRegistry,
            PotentialMatchStreamingService potentialMatchStreamingService,
            @Qualifier("ioExecutorService") ExecutorService ioExecutor,
            @Qualifier("cpuExecutor") ExecutorService cpuExecutor,
            GraphPreProcessor graphPreProcessor
    ) {
        this.perfectMatchSaver = perfectMatchSaver;
        this.strategySelector = strategySelector;
        this.matchingConfigurationRepository = matchingConfigurationRepository;
        this.meterRegistry = meterRegistry;
        this.ioExecutor = ioExecutor;
        this.cpuExecutor = cpuExecutor;
        this.graphPreProcessor = graphPreProcessor;
        this.potentialMatchStreamingService = potentialMatchStreamingService;
        this.cpuTaskSemaphore = new Semaphore(Runtime.getRuntime().availableProcessors() * 2);
        meterRegistry.gauge("adjacency_map_current_size", currentAdjacencyMapSize);
    }

    @Override
    public CompletableFuture<Void> processAndSaveMatches(MatchingRequest request) {
        final UUID groupId = request.getGroupId();
        final UUID domainId = request.getDomainId();
        final String cycleId = request.getProcessingCycleId();
        final Timer.Sample sample = Timer.start(meterRegistry);
        memoryExceeded.set(false);

        return CompletableFuture.supplyAsync(() -> {
                    Optional<MatchingConfiguration> configOptional =
                            matchingConfigurationRepository.findByGroupIdAndDomainId(groupId, domainId);
                    if (configOptional.isEmpty()) {
                        log.warn("No MatchingConfiguration for groupId={}, domainId={}", groupId, domainId);
                        throw new IllegalStateException("Missing matching configuration");
                    }
                    MatchingConfiguration config = configOptional.get();
                    MatchingGroup group = config.getGroup();
                    MatchType matchType = graphPreProcessor.determineMatchType(groupId, domainId);

                    return GraphRequestFactory.buildMatchingContext(
                            groupId, domainId, 0, matchType, group.isCostBased(), group.getIndustry(), request
                    );
                }, ioExecutor)
                .thenCompose(matchingContext -> processMatchesWithCursor(matchingContext, groupId, domainId, cycleId))
                .whenComplete((v, t) -> {
                    sample.stop(meterRegistry.timer(
                            "matching_duration",
                            "groupId", groupId.toString(),
                            "domainId", domainId.toString(),
                            "cycleId", cycleId
                    ));
                    if (t != null) {
                        log.error("Matching failed for groupId={}, domainId={}, cycleId={}: {}",
                                groupId, domainId, cycleId, t.getMessage(), t);
                        meterRegistry.counter(
                                "matching_errors_total",
                                "groupId", groupId.toString(),
                                "domainId", domainId.toString(),
                                "cycleId", cycleId
                        ).increment();
                    } else {
                        log.info("Matching completed successfully for groupId={}, domainId={}, cycleId={}",
                                groupId, domainId, cycleId);
                    }
                });
    }

    @Transactional
    public CompletableFuture<Void> processMatchesWithCursor(
            MatchingContext context, UUID groupId, UUID domainId, String cycleId) {

        final MatchingStrategy strategy = strategySelector.select(context, groupId);
        List<CompletableFuture<Void>> pageProcessingFutures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger pageCount = new AtomicInteger();
        AtomicLong totalRecordsProcessed = new AtomicLong();

        try {
            potentialMatchStreamingService.streamAllMatches(groupId, domainId, batchToProcess -> {
                if (memoryExceeded.get()) {
                    throw new CancellationException("Processing cancelled due to memory limit exceeded.");
                }

                try {
                    cpuTaskSemaphore.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException("Interrupted while acquiring CPU task semaphore.");
                }

                final int currentPageIndex = pageCount.getAndIncrement();
                totalRecordsProcessed.addAndGet(batchToProcess.size());
                log.debug("Received batch of size={} for groupId={}, domainId={}", batchToProcess.size(), groupId, domainId);

                CompletableFuture<Void> batchFuture = CompletableFuture.runAsync(() -> {
                    try {
                        if (memoryExceeded.get()) {
                            throw new CancellationException("Processing cancelled due to memory limit exceeded.");
                        }

                        int subBatchSize = adjustBatchSize();
                        List<List<PotentialMatchEntity>> subBatches = partition(batchToProcess, subBatchSize);

                        for (List<PotentialMatchEntity> subBatch : subBatches) {
                            Map<String, List<PotentialMatchEntity>> grouped = new HashMap<>();
                            for (PotentialMatchEntity pm : subBatch) {
                                if (pm.getReferenceId() != null) {
                                    grouped.computeIfAbsent(pm.getReferenceId(), k -> new ArrayList<>()).add(pm);
                                }
                                if (pm.getMatchedReferenceId() != null) {
                                    grouped.computeIfAbsent(pm.getMatchedReferenceId(), k -> new ArrayList<>()).add(pm);
                                }
                            }

                            AtomicLong nodeCount = new AtomicLong(0);
                            List<GraphRecords.PotentialMatch> nodeMatches = new ArrayList<>();
                            Iterator<Map.Entry<String, List<PotentialMatchEntity>>> nodeIterator = grouped.entrySet().iterator();
                            int nodesProcessed = 0;

                            while (nodeIterator.hasNext() && nodesProcessed < NODES_PER_PROCESSING_BATCH) {
                                if (isMemoryThresholdExceeded()) {
                                    memoryExceeded.set(true);
                                    pageProcessingFutures.forEach(future -> future.cancel(true));
                                    cpuExecutor.shutdownNow();
                                    throw new IllegalStateException("Memory usage exceeds limit: " + (maxMemoryMb * MEMORY_THRESHOLD_RATIO) + "MB");
                                }

                                Map.Entry<String, List<PotentialMatchEntity>> entry = nodeIterator.next();
                                String currentNodeId = entry.getKey();
                                List<PotentialMatchEntity> allMatchesForNode = entry.getValue();
                                PriorityQueue<GraphRecords.PotentialMatch> queue = new PriorityQueue<>(
                                        maxMatchesPerNode + 1,
                                        Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore).reversed()
                                );

                                try {
                                    for (PotentialMatchEntity pm : allMatchesForNode) {
                                        if (Thread.currentThread().isInterrupted() || memoryExceeded.get()) {
                                            throw new CancellationException("Graph construction interrupted or memory exceeded for groupId=" + groupId + ", page=" + currentPageIndex);
                                        }
                                        if (pm.getReferenceId() == null || pm.getMatchedReferenceId() == null) {
                                            log.warn("Skipping invalid match: refId={}, matchedRefId={}", pm.getReferenceId(), pm.getMatchedReferenceId());
                                            continue;
                                        }

                                        if (currentNodeId.equals(pm.getReferenceId())) {
                                            queue.offer(new GraphRecords.PotentialMatch(
                                                    pm.getReferenceId(),
                                                    pm.getMatchedReferenceId(),
                                                    pm.getCompatibilityScore(),
                                                    pm.getGroupId(),
                                                    pm.getDomainId()));
                                        }
                                        if (currentNodeId.equals(pm.getMatchedReferenceId())) {
                                            queue.offer(new GraphRecords.PotentialMatch(
                                                    pm.getMatchedReferenceId(),
                                                    pm.getReferenceId(),
                                                    pm.getCompatibilityScore(),
                                                    pm.getGroupId(),
                                                    pm.getDomainId()));
                                        }
                                        if (queue.size() > maxMatchesPerNode) {
                                            queue.poll();
                                        }
                                    }

                                    nodeMatches.addAll(queue);
                                    nodesProcessed++;
                                    if (nodeCount.incrementAndGet() > MAX_NODES_PER_BATCH) {
                                        throw new IllegalStateException("Node count exceeds limit: " + MAX_NODES_PER_BATCH);
                                    }
                                } finally {
                                    queue.clear();
                                }

                                if (nodesProcessed >= NODES_PER_PROCESSING_BATCH || !nodeIterator.hasNext()) {
                                    Map<String, PriorityQueue<GraphRecords.PotentialMatch>> nodeMap = new ConcurrentHashMap<>();
                                    nodeMatches.forEach(match -> nodeMap.computeIfAbsent(
                                            match.getReferenceId(),
                                            k -> new PriorityQueue<>(maxMatchesPerNode + 1, Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore).reversed())
                                    ).offer(match));
                                    currentAdjacencyMapSize.set(nodeMap.size());
                                    meterRegistry.counter("stream_records_processed_total",
                                            Tags.of("groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId)
                                    ).increment(nodeMatches.size());
                                    processPageMatches(nodeMap, strategy, groupId, domainId, cycleId);
                                    nodeMatches.clear();
                                }
                            }
                        }
                    } finally {
                        cpuTaskSemaphore.release();
                    }
                }, cpuExecutor).orTimeout(PAGE_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                pageProcessingFutures.add(batchFuture);
            }, BATCH_SIZE_FROM_CURSOR);
        } catch (CancellationException e) {
            log.warn("Streaming cancelled for groupId={}, domainId={}, cycleId={}: {}", groupId, domainId, cycleId, e.getMessage());
        }

        return CompletableFuture.allOf(pageProcessingFutures.toArray(new CompletableFuture[0]))
                .whenComplete((v, t) -> {
                    currentAdjacencyMapSize.set(0);
                    if (t != null) {
                        log.error("Error during batch processing of matches for groupId={}, domainId={}, cycleId={}: {}",
                                groupId, domainId, cycleId, t.getMessage(), t);
                        pageProcessingFutures.forEach(future -> future.cancel(true));
                        cpuExecutor.shutdownNow();
                    } else {
                        log.info("All batches processed and saved for groupId={}, domainId={}, cycleId={}",
                                groupId, domainId, cycleId);
                    }
                });
    }

    private int adjustBatchSize() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxAllowedMemory = (long) (maxMemoryMb * 1024 * 1024 * MEMORY_THRESHOLD_RATIO);
        int adjustedSize = BASE_SUB_BATCH_SIZE;
        if (usedMemory > maxAllowedMemory * 0.5) {
            adjustedSize = Math.max(maxMatchesPerNode, BASE_SUB_BATCH_SIZE / 2);
        }
        log.debug("Adjusted sub-batch size: {}", adjustedSize);
        return adjustedSize;
    }

    private boolean isMemoryThresholdExceeded() {
        Runtime runtime = Runtime.getRuntime();
        long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        return usedMb > (maxMemoryMb * MEMORY_THRESHOLD_RATIO);
    }

    private CompletableFuture<Void> processPageMatches(
            Map<String, PriorityQueue<GraphRecords.PotentialMatch>> nodeMap,
            MatchingStrategy strategy,
            UUID groupId,
            UUID domainId,
            String cycleId
    ) {
        List<CompletableFuture<Void>> saveFutures = new ArrayList<>();
        int matchBatchSize = adjustBatchSize();
        List<PerfectMatchEntity> buffer = new ArrayList<>(maxMatchesPerNode);

        log.debug("Processing {} nodes with {} matches for groupId={}, domainId={}",
                nodeMap.size(), nodeMap.values().stream().mapToLong(PriorityQueue::size).sum(), groupId, domainId);

        Map<String, List<MatchResult>> batchResult = strategy.match(
                nodeMap.values().stream().flatMap(PriorityQueue::stream).toList(), groupId, domainId);

        for (Map.Entry<String, List<MatchResult>> resultEntry : batchResult.entrySet()) {
            for (MatchResult match : resultEntry.getValue()) {
                PerfectMatchEntity entity = PerfectMatchEntity.builder()
                        .groupId(groupId)
                        .referenceId(resultEntry.getKey())
                        .matchedReferenceId(match.getPartnerId())
                        .compatibilityScore(match.getScore())
                        .domainId(domainId)
                        .matchedAt(DefaultValuesPopulator.getCurrentTimestamp())
                        .build();
                buffer.add(entity);

                if (buffer.size() >= maxMatchesPerNode) {
                    CompletableFuture<Void> saveFuture = perfectMatchSaver.saveMatchesAsync(new ArrayList<>(buffer), groupId, domainId, cycleId)
                            .orTimeout(SAVE_MATCHES_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .thenRun(() -> {
                                meterRegistry.counter(
                                        "perfect_matches_saved_total",
                                        Tags.of("groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId)
                                ).increment(buffer.size());
                                log.info("Saved {} perfect matches for groupId={}, domainId={}, cycleId={}.",
                                        buffer.size(), groupId, domainId, cycleId);
                            });
                    saveFutures.add(saveFuture);
                    buffer.clear();
                }
            }
        }

        if (!buffer.isEmpty()) {
            CompletableFuture<Void> saveFuture = perfectMatchSaver.saveMatchesAsync(new ArrayList<>(buffer), groupId, domainId, cycleId)
                    .orTimeout(SAVE_MATCHES_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .thenRun(() -> {
                        meterRegistry.counter(
                                "perfect_matches_saved_total",
                                Tags.of("groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId)
                        ).increment(buffer.size());
                        log.info("Saved {} perfect matches for groupId={}, domainId={}, cycleId={}",
                                buffer.size(), groupId, domainId, cycleId);
                    });
            saveFutures.add(saveFuture);
        }

        return CompletableFuture.allOf(saveFutures.toArray(new CompletableFuture[0]));
    }
}