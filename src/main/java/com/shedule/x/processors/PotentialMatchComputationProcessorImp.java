package com.shedule.x.processors;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.config.QueueManagerConfig;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.config.factory.QueueManagerFactory;
import com.shedule.x.models.Edge;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class PotentialMatchComputationProcessorImp implements PotentialMatchComputationProcessor {
    private final GraphStore graphStore;
    private final PotentialMatchSaver potentialMatchSaver;
    private final MeterRegistry meterRegistry;
    private final ExecutorService mappingExecutor;
    private final ExecutorService storageExecutor;
    private final ExecutorService shutdownExecutor;
    private final Semaphore saveSemaphore;
    private final QueueManagerFactory queueManagerFactory;
    private final QueueManagerConfig queueManagerConfig;
    private volatile boolean shutdownInitiated = false;
    private final AtomicInteger lastFlushIntervalSeconds = new AtomicInteger(0);

    @Value("${match.flush.min-interval-seconds:2}")
    private int minFlushIntervalSeconds;

    @Value("${match.flush.queue-threshold:0.8}")
    private double flushQueueThreshold;

    @Value("${match.save.timeout-seconds:300}")
    private int matchSaveTimeoutSeconds;

    @Value("${match.final-save.timeout-seconds:60}")
    private int finalSaveTimeoutSeconds;

    @Value("${match.temp-table.batch-size:200}")
    private int tempTableBatchSize;

    public PotentialMatchComputationProcessorImp(
            GraphStore graphStore,
            PotentialMatchSaver potentialMatchSaver,
            MeterRegistry meterRegistry,
            @Qualifier("persistenceExecutor") ExecutorService mappingExecutor,
            @Qualifier("matchesProcessExecutor") ExecutorService storageExecutor,
            QueueManagerFactory queueManagerFactory,
            @Value("${match.semaphore.permits:8}") int semaphorePermits,
            @Value("${match.queue.capacity:1000000}") int queueCapacity,
            @Value("${match.flush.interval-seconds:5}") int flushIntervalSeconds,
            @Value("${match.queue.drain-warning-threshold:0.9}") double drainWarningThreshold,
            @Value("${match.boost-batch-factor:2}") int boostBatchFactor,
            @Value("${match.max-final-batch-size:50000}") int maxFinalBatchSize) {
        this.graphStore = Objects.requireNonNull(graphStore, "graphStore must not be null");
        this.potentialMatchSaver = Objects.requireNonNull(potentialMatchSaver, "potentialMatchSaver must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.mappingExecutor = mappingExecutor;
        this.storageExecutor = storageExecutor;
        this.shutdownExecutor = Executors.newSingleThreadExecutor();
        this.saveSemaphore = new Semaphore(semaphorePermits, true);
        this.queueManagerFactory = queueManagerFactory;
        this.queueManagerConfig = new QueueManagerConfig(
                queueCapacity, flushIntervalSeconds, drainWarningThreshold, boostBatchFactor, maxFinalBatchSize);
        this.lastFlushIntervalSeconds.set(flushIntervalSeconds);
    }

    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;
        mappingExecutor.shutdown();
        storageExecutor.shutdown();
        try {
            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> {
                QueueManagerImpl.flushAllQueuesBlocking(this::savePendingMatchesBlocking);
                QueueManagerImpl.removeAll();
                resultFuture.complete(null);
            }, shutdownExecutor).orTimeout(300, TimeUnit.SECONDS)
                    .exceptionally(t -> {
                        log.error("Shutdown task failed: {}", t.getMessage(), t);
                        resultFuture.completeExceptionally(t);
                        return null;
                    });

            resultFuture.get(30, TimeUnit.SECONDS);

            if (!mappingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                mappingExecutor.shutdownNow();
            }
            if (!storageExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                storageExecutor.shutdownNow();
            }
            shutdownExecutor.shutdown();
            if (!shutdownExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                shutdownExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Shutdown failed: {}", e.getMessage(), e);
        }
    }

    public CompletableFuture<Void> processChunkMatchesFallback(
            GraphRecords.ChunkResult chunkResult, UUID groupId, UUID domainId,
            String processingCycleId, int batchSize, Throwable throwable) {
        log.warn(
                "Circuit breaker triggered: Failed to process chunk matches for groupId={}, chunkIndex={}, processingCycleId={}: {}",
                groupId, chunkResult.getChunkIndex(), processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_circuit_breaker", "groupId", groupId.toString()).increment();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> savePendingMatches(
            UUID groupId,
            UUID domainId,
            String processingCycleId,
            int batchSize) {
        if (shutdownInitiated || storageExecutor.isShutdown()) {
            log.warn("Pending match save aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.completedFuture(null);
        }

        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, processingCycleId,
                queueManagerConfig);
        QueueManagerImpl manager = queueManagerFactory.create(queueConfig);

        return processBatchIfNeeded(manager, null, batchSize, groupId, domainId, processingCycleId)
                .exceptionallyCompose(throwable -> savePendingMatchesFallback(groupId, domainId, processingCycleId,
                        batchSize, throwable));
    }

    public CompletableFuture<Void> savePendingMatchesFallback(
            UUID groupId, UUID domainId, String processingCycleId, int batchSize, Throwable throwable) {
        log.warn("Fallback triggered: Failed to save pending matches for groupId={}, processingCycleId={}: {}",
                groupId, processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(), "error_type", "save")
                .increment();
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> saveFinalMatchesFallback(
            UUID groupId, UUID domainId, String processingCycleId, AutoCloseableStream<Edge> stream,
            Throwable throwable) {
        log.warn("Fallback triggered: Failed to save final matches for groupId={}, processingCycleId={}: {}",
                groupId, processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(), "error_type", "final_save")
                .increment();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Set<String> getCachedMatchKeysForDomainAndGroup(UUID groupId, UUID domainId) {
        Instant start = Instant.now();
        log.debug("Listing match keys for groupId={}, domainId={}", groupId, domainId);

        try {
            return graphStore.getKeysByGroupAndDomain(groupId, domainId);
        } catch (Exception e) {
            log.error("Failed to list match keys for groupId={}, domainId={}: {}", groupId, domainId, e.getMessage());
            meterRegistry.counter("match_processor_list_keys_errors", "groupId", groupId.toString()).increment();
            return Set.of();
        }
    }

    @Override
    public AutoCloseableStream<Edge> streamEdges(UUID groupId, UUID domainId, String processingCycleId, int topK) {
        Instant start = Instant.now();
        log.debug("Streaming edges for groupId={}, domainId={}, processingCycleId={}", groupId, domainId,
                processingCycleId);

        try {
            AutoCloseableStream<Edge> edgeStream = new AutoCloseableStream<>(
                    graphStore.streamEdges(domainId, groupId).getStream());
            long edgeCount = edgeStream.getStream().count();
            edgeStream = new AutoCloseableStream<>(graphStore.streamEdges(domainId, groupId).getStream());
            log.info("Streaming {} edges for groupId={}, domainId={}, processingCycleId={}", edgeCount, groupId,
                    domainId, processingCycleId);
            return new AutoCloseableStream<>(edgeStream.getStream().onClose(() -> {
                meterRegistry
                        .timer("match_processor_stream_latency", "groupId", groupId.toString(), "processingCycleId",
                                processingCycleId)
                        .record(Duration.between(start, Instant.now()));
            }));
        } catch (Exception e) {
            log.error("Failed to stream edges for groupId={}, domainId={}, processingCycleId={}: {}", groupId, domainId,
                    processingCycleId, e.getMessage());
            meterRegistry.counter("match_processor_stream_errors", "groupId", groupId.toString(), "processingCycleId",
                    processingCycleId).increment();
            return graphStore.streamEdgesFallback(domainId, groupId, topK, e);
        }
    }

    private CompletableFuture<Void> processBatchIfNeeded(
            QueueManagerImpl manager,
            GraphRecords.ChunkResult chunkResult,
            int batchSize,
            UUID groupId,
            UUID domainId,
            String processingCycleId) {
        BlockingQueue<GraphRecords.PotentialMatch> pendingMatches = manager.getQueue();
        List<GraphRecords.PotentialMatch> batch = new ArrayList<>();
        int drained = pendingMatches.drainTo(batch, batchSize);
        log.debug("Drained {} matches for groupId={}, processingCycleId={}", drained, groupId, processingCycleId);

        if (drained > 0) {
            log.info("Processing batch of {} matches for groupId={}, processingCycleId={}", drained, groupId,
                    processingCycleId);
            return saveMatchBatch(batch, groupId, domainId, processingCycleId,
                    chunkResult != null ? chunkResult.getChunkIndex() : 0);
        } else {
            log.debug("No matches drained for groupId={}, processingCycleId={}", groupId, processingCycleId);
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> saveMatchBatchFallback(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId,
            UUID domainId,
            String processingCycleId,
            int chunkIndex,
            Throwable throwable) {
        log.warn(
                "Fallback triggered: Failed to save match batch to GraphStore for groupId={}, processingCycleId={}: {}",
                groupId, processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(), "error_type", "save")
                .increment();
        cleanup(groupId);
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> savePendingMatchesBlocking(
            UUID groupId, UUID domainId, Integer batchSize, String processingCycleId) {
        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, processingCycleId,
                queueManagerConfig);
        QueueManagerImpl manager = queueManagerFactory.create(queueConfig);

        BlockingQueue<GraphRecords.PotentialMatch> queue = manager.getQueue();
        long startTime = System.currentTimeMillis();
        long totalProcessed = 0;
        int maxDrain = queueManagerConfig.getMaxFinalBatchSize();

        try {
            do {
                List<GraphRecords.PotentialMatch> batch = new ArrayList<>();
                int drained = queue.drainTo(batch, maxDrain);
                if (drained == 0) {
                    log.debug("No matches to drain for blocking save for groupId={}", groupId);
                    break;
                }

                log.info("Saving {} blocking matches to GraphStore for groupId={}", drained, groupId);
                totalProcessed += drained;

                CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                graphStore.persistEdgesAsync(batch, groupId, 0)
                        .orTimeout(matchSaveTimeoutSeconds, TimeUnit.SECONDS)
                        .thenAcceptAsync(result -> {
                            log.info(
                                    "Saved {} matches in blocking flush to GraphStore for groupId={}, processingCycleId={}",
                                    drained, groupId, processingCycleId);
                            meterRegistry
                                    .counter("match_saves_total", "groupId", groupId.toString(), "type", "blocking")
                                    .increment(drained);
                            resultFuture.complete(null);
                        }, mappingExecutor)
                        .exceptionally(throwable -> {
                            log.error("Blocking save to GraphStore failed for groupId={}, processingCycleId={}: {}",
                                    groupId, processingCycleId, throwable.getMessage());
                            meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(),
                                    "error_type", "blocking_save").increment();
                            resultFuture.completeExceptionally(throwable);
                            return null;
                        });

                resultFuture.get(matchSaveTimeoutSeconds, TimeUnit.SECONDS);

                if (System.currentTimeMillis() - startTime > matchSaveTimeoutSeconds * 1000L) {
                    long remaining = queue.size();
                    log.warn("Shutdown limit of {} seconds reached, {} matches remaining for groupId={}",
                            matchSaveTimeoutSeconds, remaining, groupId);
                    meterRegistry.counter("matches_dropped_due_to_shutdown", "groupId", groupId.toString(), "domainId",
                            domainId.toString()).increment(remaining);
                    break;
                }
            } while (!queue.isEmpty());

            log.info(
                    "Completed blocking save to GraphStore for groupId={}, processed {} matches, remaining queue size={}",
                    groupId, totalProcessed, queue.size());
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            log.error("Blocking save to GraphStore failed for groupId={}: {}", groupId, e.getMessage());
            throw new CompletionException("Blocking save failed", e);
        }
    }

    @Override
    public CompletableFuture<Void> saveFinalMatches(
            UUID groupId, UUID domainId, String processingCycleId, AutoCloseableStream<Edge> initialEdgeStream,
            int topK) {
        log.info("Starting final save for groupId={}, topK={}", groupId, topK);

        return CompletableFuture.runAsync(() -> {
            try {
                if (!saveSemaphore.tryAcquire()) {
                    log.warn("Failed to acquire saveSemaphore for groupId={}", groupId);
                    throw new IllegalStateException("Unable to acquire semaphore for final save");
                }
                log.debug("Acquired saveSemaphore for groupId={}, permits left={}", groupId,
                        saveSemaphore.availablePermits());
                try (AutoCloseableStream<Edge> edgeStream = graphStore.streamEdges(domainId, groupId)) {
                    long edgeCount = edgeStream.getStream().count();
                    log.info("Available edges before processing: {} for groupId={}", edgeCount, groupId);
                }
                try (AutoCloseableStream<Edge> processingStream = graphStore.streamEdges(domainId, groupId)) {
                    List<PotentialMatchEntity> batch = new ArrayList<>(tempTableBatchSize);
                    AtomicLong count = new AtomicLong(0);
                    Map<String, List<PotentialMatchEntity>> matchesByRefId = new HashMap<>();
                    processingStream.forEach(edge -> {
                        try {
                            PotentialMatchEntity entity = GraphRequestFactory.convertToPotentialMatch(edge, groupId,
                                    domainId, processingCycleId);
                            if (entity != null) {
                                matchesByRefId.computeIfAbsent(entity.getReferenceId(), k -> new ArrayList<>())
                                        .add(entity);
                                count.incrementAndGet();
                            } else {
                                log.warn("Null entity for edge: refId={}, matchedRefId={}, groupId={}",
                                        edge.getFromNode().getReferenceId(), edge.getToNode().getReferenceId(),
                                        groupId);
                            }
                        } catch (Exception e) {
                            log.error("Failed to convert edge for groupId={}: {}", groupId, e.getMessage());
                            meterRegistry.counter("match_conversion_errors", "groupId", groupId.toString()).increment();
                        }
                    });
                    log.info("Processed {} edges for {} nodes in groupId={}", count.get(), matchesByRefId.size(),
                            groupId);
                    List<CompletableFuture<Void>> saveFutures = new ArrayList<>();
                    for (List<PotentialMatchEntity> matches : matchesByRefId.values()) {
                        batch.addAll(matches);
                        if (batch.size() >= tempTableBatchSize) {
                            log.debug("Saving batch of {} matches to database for groupId={}", batch.size(), groupId);
                            List<PotentialMatchEntity> batchToSave = new ArrayList<>(batch);
                            saveFutures.add(potentialMatchSaver.saveMatchesAsync(batchToSave, groupId, domainId,
                                    processingCycleId, true));
                            batch.clear();
                        }
                    }
                    if (!batch.isEmpty()) {
                        log.debug("Saving final batch of {} matches to database for groupId={}", batch.size(), groupId);
                        saveFutures.add(potentialMatchSaver.saveMatchesAsync(batch, groupId, domainId,
                                processingCycleId, true));
                    }

                    CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                    CompletableFuture.allOf(saveFutures.toArray(new CompletableFuture[0]))
                            .thenAcceptAsync(v -> {
                                if (matchesByRefId.size() < 500) {
                                    log.warn("Only {} nodes processed (expected ~500) in groupId={}",
                                            matchesByRefId.size(), groupId);
                                    meterRegistry.counter("nodes_without_matches", "groupId", groupId.toString())
                                            .increment(500 - matchesByRefId.size());
                                }
                                log.info("Saved {} matches for {} nodes to database for groupId={}", count.get(),
                                        matchesByRefId.size(), groupId);
                                resultFuture.complete(null);
                            }, storageExecutor)
                            .exceptionally(t -> {
                                log.error("Failed to save matches for groupId={}: {}", groupId, t.getMessage());
                                resultFuture.completeExceptionally(t);
                                return null;
                            });

                    try {
                        resultFuture.get(finalSaveTimeoutSeconds, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        log.error("Execution error during final save for groupId={}: {}", groupId,
                                e.getCause().getMessage());
                        throw new CompletionException("Execution error during final save", e.getCause());
                    } catch (TimeoutException e) {
                        log.error("Timeout during final save for groupId={}: {}", groupId, e.getMessage());
                        throw new CompletionException("Timeout during final save", e);
                    }
                }
            } catch (InterruptedException e) {
                log.error("Interrupted final save for groupId={}: {}", groupId, e.getMessage());
                Thread.currentThread().interrupt();
                throw new CompletionException("Interrupted final save", e);
            } finally {
                saveSemaphore.release();
                log.debug("Released saveSemaphore for groupId={}", groupId);
            }
        }, storageExecutor)
                .orTimeout(finalSaveTimeoutSeconds, TimeUnit.SECONDS)
                .exceptionallyCompose(throwable -> saveFinalMatchesFallback(groupId, domainId, processingCycleId,
                        initialEdgeStream, throwable));
    }

    @Override
    public void cleanup(UUID groupId) {
        try {
            QueueManagerImpl.flushQueueBlocking(groupId, this::savePendingMatchesBlocking);
            QueueManagerImpl.remove(groupId);
            log.info("Deleting database matches for groupId={}", groupId);

            potentialMatchSaver.deleteByGroupId(groupId)
                    .thenAcceptAsync(v -> log.info("Successfully deleted matches for groupId={}", groupId))
                    .exceptionally(throwable -> {
                        log.error("Failed to delete matches for groupId={}: {}", groupId, throwable.getMessage());
                        return null;
                    });
            log.info("Deferred GraphStore cleanup for groupId={}", groupId);
        } catch (Exception e) {
            log.error("Cleanup failed for groupId={}: {}", groupId, e.getMessage());
            throw new CompletionException("Cleanup failed", e);
        }
    }

    @Override
    public long getFinalMatchCount(UUID groupId, UUID domainId, String processingCycleId) {
        Instant start = Instant.now();
        log.debug("Counting final matches for groupId={}, domainId={}, processingCycleId={}", groupId, domainId,
                processingCycleId);

        try (AutoCloseableStream<Edge> edgeStream = graphStore.streamEdges(domainId, groupId)) {
            long count = edgeStream.getStream().count();
            meterRegistry
                    .timer("match_processor_count_latency", "groupId", groupId.toString(), "processingCycleId",
                            processingCycleId)
                    .record(Duration.between(start, Instant.now()));
            log.info("Final match count for groupId={}, domainId={}, processingCycleId={}: {}", groupId, domainId,
                    processingCycleId, count);

            try {
                graphStore.cleanEdges(groupId);
                log.info("Cleaned GraphStore edges for groupId={}", groupId);
            } catch (Exception e) {
                log.error("Failed to clean edges in GraphStore for groupId={}: {}", groupId, e.getMessage());
                graphStore.cleanEdgesFallback(groupId, e);
            }

            return count;
        } catch (Exception e) {
            log.error("Failed to count final matches for groupId={}, domainId={}, processingCycleId={}: {}",
                    groupId, domainId, processingCycleId, e.getMessage());
            meterRegistry.counter("match_processor_count_errors", "groupId", groupId.toString(), "processingCycleId",
                    processingCycleId).increment();
            return 0;
        }
    }

    private CompletableFuture<Void> saveMatchBatch(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId,
            UUID domainId,
            String processingCycleId,
            int chunkIndex) {
        if (matches.isEmpty()) {
            log.debug("Empty batch for groupId={}, processingCycleId={}", groupId, processingCycleId);
            return CompletableFuture.completedFuture(null);
        }
        if (shutdownInitiated || storageExecutor.isShutdown()) {
            log.warn("Match batch save aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.completedFuture(null);
        }

        log.info("Saving {} matches for groupId={}, processingCycleId={}", matches.size(), groupId, processingCycleId);
        Instant saveStart = Instant.now();

        List<PotentialMatchEntity> entities = matches.stream()
                .map(GraphRequestFactory::convertToPotentialMatch)
                .collect(Collectors.toList());

        CompletableFuture<Void> dbSave = CompletableFuture.supplyAsync(() -> {
            try {
                potentialMatchSaver.saveMatchesAsync(entities, groupId, domainId, processingCycleId, false);
            } catch (Exception e) {
                log.warn("Bulk save failed, falling back to JDBC batch for groupId={}: {}", groupId, e.getMessage());
            }
            return null;
        }, storageExecutor);

        CompletableFuture<Void> graphStoreSave = graphStore.persistEdgesAsync(matches, groupId, chunkIndex)
                .orTimeout(matchSaveTimeoutSeconds, TimeUnit.SECONDS)
                .whenComplete((result, throwable) -> {
                    meterRegistry
                            .timer("graph_builder_save", "groupId", groupId.toString(), "processingCycleId",
                                    processingCycleId,
                                    "status", throwable == null ? "success" : "failure")
                            .record(Duration.between(saveStart, Instant.now()));
                    if (throwable == null) {
                        log.info("Saved {} matches to GraphStore for groupId={}, processingCycleId={}",
                                matches.size(), groupId, processingCycleId);
                        meterRegistry
                                .counter("match_saves_total", "groupId", groupId.toString(), "processingCycleId",
                                        processingCycleId)
                                .increment(matches.size());
                    } else {
                        log.error("Failed to save batch to GraphStore for groupId={}, processingCycleId={}: {}",
                                groupId, processingCycleId, throwable.getMessage());
                        meterRegistry
                                .counter("match_processing_errors", "groupId", groupId.toString(), "error_type", "save")
                                .increment();
                    }
                })
                .exceptionallyCompose(throwable -> graphStore.persistEdgesAsyncFallback(matches, groupId.toString(),
                        chunkIndex, throwable));

        return CompletableFuture.allOf(dbSave, graphStoreSave);
    }

    @Override
    public CompletableFuture<Void> processChunkMatches(
            GraphRecords.ChunkResult chunkResult,
            UUID groupId,
            UUID domainId,
            String processingCycleId,
            int matchBatchSize) {
        if (shutdownInitiated || mappingExecutor.isShutdown()) {
            log.warn("Chunk match processing aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.completedFuture(null);
        }

        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, processingCycleId,
                queueManagerConfig);
        QueueManagerImpl manager = queueManagerFactory.create(queueConfig);

        BlockingQueue<GraphRecords.PotentialMatch> pendingMatches = manager.getQueue();

        double queueLoad = (double) pendingMatches.size() / queueManagerConfig.getCapacity();
        int newFlushInterval = queueLoad > flushQueueThreshold
                ? Math.max(minFlushIntervalSeconds, queueManagerConfig.getFlushIntervalSeconds() / 5)
                : queueManagerConfig.getFlushIntervalSeconds();
        if (lastFlushIntervalSeconds.compareAndSet(lastFlushIntervalSeconds.get(), newFlushInterval)) {
            manager.setFlushInterval(newFlushInterval);
            log.debug("Set flush interval to {}s for groupId={} due to queue load={}", newFlushInterval, groupId,
                    queueLoad);
        }

        List<GraphRecords.PotentialMatch> matches = chunkResult.getMatches();
        if (matches == null || matches.isEmpty()) {
            log.warn(
                    "Received empty or null matches in ChunkResult for groupId={}, chunkIndex={}, processingCycleId={}",
                    groupId, chunkResult.getChunkIndex(), processingCycleId);
            meterRegistry.counter("match_drops_total", "groupId", groupId.toString(), "reason", "empty_chunk",
                    "match_type", "chunk").increment();
            return CompletableFuture.completedFuture(null);
        }

        Set<String> matchKeys = new HashSet<>();
        List<GraphRecords.PotentialMatch> uniqueMatches = matches.stream()
                .filter(m -> matchKeys.add(m.getReferenceId() + ":" + m.getMatchedReferenceId()))
                .toList();

        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Processing {} matches for groupId={}, chunkIndex={}, processingCycleId={}",
                        uniqueMatches.size(), groupId, chunkResult.getChunkIndex(), processingCycleId);
                Instant mappingStart = Instant.now();
                int offeredCount = 0;
                for (GraphRecords.PotentialMatch match : uniqueMatches) {
                    try {
                        log.debug(
                                "Attempting to queue match: groupId={}, refId={}, matchedRefId={}, score={}, processingCycleId={}",
                                groupId, match.getReferenceId(), match.getMatchedReferenceId(),
                                match.getCompatibilityScore(), processingCycleId);
                        if (!pendingMatches.offer(match, 100, TimeUnit.MILLISECONDS)) {
                            log.warn("Queue offer timed out for groupId={}, processingCycleId={}", groupId,
                                    processingCycleId);
                            meterRegistry.counter("match_drops_total", "groupId", groupId.toString(), "reason",
                                    "queue_timeout", "match_type", "chunk").increment();
                            continue;
                        }
                        offeredCount++;
                        log.debug("Queued match: groupId={}, refId={}, matchedRefId={}, processingCycleId={}",
                                groupId, match.getReferenceId(), match.getMatchedReferenceId(), processingCycleId);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("Interrupted adding match to queue for groupId={}, processingCycleId={}", groupId,
                                processingCycleId);
                        meterRegistry.counter("match_drops_total", "groupId", groupId.toString(), "reason",
                                "queue_interrupted", "match_type", "chunk").increment();
                    }
                }
                log.info("Queued {} of {} matches for groupId={}, processingCycleId={}, queueSize={}",
                        offeredCount, uniqueMatches.size(), groupId, processingCycleId, pendingMatches.size());
                if (offeredCount < uniqueMatches.size()) {
                    log.warn("Dropped {} matches from chunk {} for groupId={}, processingCycleId={}",
                            uniqueMatches.size() - offeredCount, chunkResult.getChunkIndex(), groupId,
                            processingCycleId);
                    meterRegistry
                            .counter("match_drops_total", "groupId", groupId.toString(), "reason",
                                    "queue_full_or_timeout", "match_type", "chunk")
                            .increment(uniqueMatches.size() - offeredCount);
                }

                meterRegistry
                        .timer("graph_builder_mapping", "groupId", groupId.toString(), "processingCycleId",
                                processingCycleId)
                        .record(Duration.between(mappingStart, Instant.now()));

                return processBatchIfNeeded(manager, chunkResult, matchBatchSize, groupId, domainId, processingCycleId);
            } catch (Exception e) {
                log.error("Match mapping failed for groupId={}, processingCycleId={}: {}", groupId, processingCycleId,
                        e.getMessage());
                throw new CompletionException("Match mapping failed", e);
            }
        }, mappingExecutor)
                .thenComposeAsync(Function.identity())
                .exceptionallyCompose(ex -> processChunkMatchesFallback(chunkResult, groupId, domainId,
                        processingCycleId, matchBatchSize, ex));
    }
}