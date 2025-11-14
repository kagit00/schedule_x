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
import com.shedule.x.utils.monitoring.MemoryMonitoringUtility;
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

        // Backpressure configuration
        final int maxConcurrentBatches = 5;
        final int backpressureTimeoutSeconds = 30;
        final Semaphore batchSemaphore = new Semaphore(maxConcurrentBatches);
        final BlockingQueue<CompletableFuture<Void>> processingQueue =
                new LinkedBlockingQueue<>(maxConcurrentBatches * 2);

        AtomicInteger pageCount = new AtomicInteger();
        AtomicLong totalRecordsProcessed = new AtomicLong();
        AtomicInteger activeBatches = new AtomicInteger();

        try {
            potentialMatchStreamingService.streamAllMatches(groupId, domainId, batchToProcess -> {
                if (memoryExceeded.get()) {
                    throw new CancellationException("Processing cancelled due to memory limit exceeded.");
                }

                if (pageCount.get() % 100 == 0) {
                    MemoryMonitoringUtility.logMemoryUsage("PotentialMatchesStreamingBatch", groupId, maxMemoryMb);
                }

                try {
                    if (!batchSemaphore.tryAcquire(backpressureTimeoutSeconds, TimeUnit.SECONDS)) {
                        throw new TimeoutException("Backpressure timeout - too many concurrent batches (" +
                                activeBatches.get() + "/" + maxConcurrentBatches + ")");
                    }

                    cpuTaskSemaphore.acquire();
                } catch (InterruptedException | TimeoutException e) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException("Interrupted while acquiring semaphores.");
                }

                final int currentPageIndex = pageCount.getAndIncrement();
                totalRecordsProcessed.addAndGet(batchToProcess.size());
                activeBatches.incrementAndGet();

                log.debug("Processing batch {} for groupId={}, size={}, activeBatches={}",
                        currentPageIndex, groupId, batchToProcess.size(), activeBatches.get());

                CompletableFuture<Void> batchFuture = CompletableFuture.runAsync(() -> {
                            try {
                                if (memoryExceeded.get()) {
                                    throw new CancellationException("Processing cancelled due to memory limit exceeded.");
                                }

                                processBatch(batchToProcess, strategy, groupId, domainId, cycleId, currentPageIndex);

                            } catch (Exception e) {
                                log.error("Batch processing failed for groupId={}, batchIndex={}: {}",
                                        groupId, currentPageIndex, e.getMessage(), e);
                                throw new CompletionException(e);
                            } finally {
                                cpuTaskSemaphore.release();
                                batchSemaphore.release();
                                activeBatches.decrementAndGet();
                            }
                        }, cpuExecutor)
                        .orTimeout(PAGE_PROCESSING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .exceptionally(throwable -> {
                            log.error("Batch {} failed for groupId={}: {}", currentPageIndex, groupId, throwable.getMessage());
                            meterRegistry.counter("batch_processing_errors",
                                    "groupId", groupId.toString(),
                                    "domainId", domainId.toString(),
                                    "cycleId", cycleId
                            ).increment();
                            return null;
                        });

                try {
                    if (!processingQueue.offer(batchFuture, 10, TimeUnit.SECONDS)) {
                        log.warn("Processing queue full, cancelling batch {} for groupId={}", currentPageIndex, groupId);
                        batchFuture.cancel(true);
                        throw new TimeoutException("Processing queue overloaded");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    batchFuture.cancel(true);
                    throw new CancellationException("Interrupted while queuing batch");
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }

            }, BATCH_SIZE_FROM_CURSOR);

        } catch (CancellationException e) {
            log.warn("Streaming cancelled for groupId={}, domainId={}, cycleId={}: {}",
                    groupId, domainId, cycleId, e.getMessage());
        } catch (Exception e) {
            log.error("Streaming failed for groupId={}, domainId={}, cycleId={}: {}",
                    groupId, domainId, cycleId, e.getMessage(), e);
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        processingQueue.drainTo(futures);

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, t) -> {
                    currentAdjacencyMapSize.set(0);
                    if (t != null) {
                        log.error("Error during batch processing for groupId={}, domainId={}, cycleId={}: {}",
                                groupId, domainId, cycleId, t.getMessage(), t);
                        futures.forEach(future -> future.cancel(true));
                    } else {
                        log.info("All batches processed successfully for groupId={}, domainId={}, cycleId={}. " +
                                        "Total batches: {}, total records: {}",
                                groupId, domainId, cycleId, pageCount.get(), totalRecordsProcessed.get());
                    }
                });
    }

    private void processBatch(
            List<PotentialMatchEntity> batchToProcess,
            MatchingStrategy strategy,
            UUID groupId,
            UUID domainId,
            String cycleId,
            int batchIndex) {

        if (MemoryMonitoringUtility.isMemoryThresholdExceeded(maxMemoryMb)) {
            memoryExceeded.set(true);
            throw new IllegalStateException("Memory threshold exceeded before processing batch " + batchIndex);
        }

        int subBatchSize = MemoryMonitoringUtility.adjustBatchSize(maxMemoryMb);
        List<List<PotentialMatchEntity>> subBatches = partition(batchToProcess, subBatchSize);

        for (int i = 0; i < subBatches.size(); i++) {
            List<PotentialMatchEntity> subBatch = subBatches.get(i);

            if (MemoryMonitoringUtility.isMemoryThresholdExceeded(maxMemoryMb)) {
                memoryExceeded.set(true);
                throw new IllegalStateException("Memory threshold exceeded in sub-batch " + i + " of batch " + batchIndex);
            }

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
                if (Thread.currentThread().isInterrupted() || memoryExceeded.get()) {
                    throw new CancellationException("Processing interrupted or memory exceeded in batch " + batchIndex);
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
                            throw new CancellationException("Graph construction interrupted for batch " + batchIndex);
                        }
                        if (pm.getReferenceId() == null || pm.getMatchedReferenceId() == null) {
                            log.warn("Skipping invalid match: refId={}, matchedRefId={}",
                                    pm.getReferenceId(), pm.getMatchedReferenceId());
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
                            k -> new PriorityQueue<>(maxMatchesPerNode + 1,
                                    Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore).reversed())
                    ).offer(match));

                    currentAdjacencyMapSize.set(nodeMap.size());
                    meterRegistry.counter("stream_records_processed_total",
                            Tags.of("groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId)
                    ).increment(nodeMatches.size());

                    processPageMatches(nodeMap, strategy, groupId, domainId, cycleId);
                    nodeMatches.clear();

                    if (batchIndex % 50 == 0) {
                        System.gc();
                    }
                }
            }

            grouped.clear();
        }
    }

    private CompletableFuture<Void> processPageMatches(
            Map<String, PriorityQueue<GraphRecords.PotentialMatch>> nodeMap,
            MatchingStrategy strategy,
            UUID groupId,
            UUID domainId,
            String cycleId
    ) {
        List<CompletableFuture<Void>> saveFutures = new ArrayList<>();
        final int bufferSizeLimit = 500;
        final int maxConcurrentSaves = 3;

        Semaphore saveSemaphore = new Semaphore(maxConcurrentSaves);
        List<PerfectMatchEntity> buffer = Collections.synchronizedList(new ArrayList<>(bufferSizeLimit));

        log.debug("Processing {} nodes with {} matches for groupId={}, domainId={}.",
                nodeMap.size(), nodeMap.values().stream().mapToLong(PriorityQueue::size).sum(), groupId, domainId);

        Map<String, List<MatchResult>> batchResult = strategy.match(
                nodeMap.values().stream().flatMap(PriorityQueue::stream).toList(), groupId, domainId);

        List<CompletableFuture<Void>> matchFutures = new ArrayList<>();

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

                if (buffer.size() >= bufferSizeLimit) {
                    CompletableFuture<Void> saveFuture = flushBufferWithBackpressure(
                            new ArrayList<>(buffer), groupId, domainId, cycleId, saveSemaphore);
                    matchFutures.add(saveFuture);
                    buffer.clear();

                    if (MemoryMonitoringUtility.isMemoryThresholdExceeded(maxMemoryMb)) {
                        memoryExceeded.set(true);
                        log.warn("Memory threshold exceeded during match processing for groupId={}", groupId);
                        break;
                    }
                }
            }

            if (memoryExceeded.get()) break;
        }

        if (!buffer.isEmpty() && !memoryExceeded.get()) {
            CompletableFuture<Void> saveFuture = flushBufferWithBackpressure(
                    new ArrayList<>(buffer), groupId, domainId, cycleId, saveSemaphore);
            matchFutures.add(saveFuture);
        }

        return CompletableFuture.allOf(matchFutures.toArray(new CompletableFuture[0]))
                .exceptionally(throwable -> {
                    log.error("Error saving matches for groupId={}: {}", groupId, throwable.getMessage());
                    return null;
                });
    }

    private CompletableFuture<Void> flushBufferWithBackpressure(
            List<PerfectMatchEntity> buffer,
            UUID groupId, UUID domainId, String cycleId,
            Semaphore saveSemaphore) {

        try {
            if (!saveSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                throw new TimeoutException("Timeout acquiring save semaphore - too many concurrent saves");
            }
        } catch (InterruptedException | TimeoutException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }

        return perfectMatchSaver.saveMatchesAsync(buffer, groupId, domainId, cycleId)
                .orTimeout(SAVE_MATCHES_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .thenRun(() -> {
                    meterRegistry.counter(
                            "perfect_matches_saved_total",
                            Tags.of("groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId)
                    ).increment(buffer.size());
                    log.debug("Flushed {} perfect matches for groupId={}", buffer.size(), groupId);
                })
                .whenComplete((result, throwable) -> {
                    saveSemaphore.release();
                    if (throwable != null) {
                        log.error("Failed to flush buffer for groupId={}: {}", groupId, throwable.getMessage());
                    }
                });
    }
}