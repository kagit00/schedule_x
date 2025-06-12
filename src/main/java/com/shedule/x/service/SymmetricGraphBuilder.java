package com.shedule.x.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.models.Edge;
import com.shedule.x.processors.PotentialMatchComputationProcessor;
import io.micrometer.core.instrument.Timer;
import com.shedule.x.builder.SymmetricEdgeBuildingStrategy;
import com.shedule.x.config.factory.SymmetricEdgeBuildingStrategyFactory;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Node;
import com.shedule.x.utils.db.BatchUtils;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class SymmetricGraphBuilder implements SymmetricGraphBuilderService {
    private final AtomicInteger chunkCounter = new AtomicInteger(0);
    private final SymmetricEdgeBuildingStrategyFactory strategyFactory;
    private final PotentialMatchComputationProcessor potentialMatchComputationProcessor;
    private final MeterRegistry meterRegistry;
    private final ExecutorService computeExecutor;
    private final ExecutorService persistenceExecutor;
    private final Cache<UUID, AtomicBoolean> cleanupGuards = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
    private Semaphore computeSemaphore;
    private Semaphore mappingSemaphore;
    private volatile boolean shutdownInitiated = false;

    @Value("${graph.max-concurrent-mappings:6}")
    private int maxConcurrentMappings;

    @Value("${graph.chunk-size:500}")
    private int chunkSize;

    @Value("${graph.max-concurrent-batches:3}")
    private int maxConcurrentBatches;

    @Value("${graph.match-batch-size:500}")
    private int matchBatchSize;

    @Value("${graph.top-k:200}")
    private int topK;

    @Value("${graph.chunk-processing-timeout-seconds:300}")
    private int chunkProcessingTimeoutSeconds;

    @Value("${graph.chunk-retry.max-attempts:3}")
    private int chunkRetryMaxAttempts;

    @Value("${graph.chunk-retry.initial-backoff-seconds:1}")
    private int chunkRetryInitialBackoffSeconds;

    public SymmetricGraphBuilder(
            SymmetricEdgeBuildingStrategyFactory strategyFactory,
            PotentialMatchComputationProcessor potentialMatchComputationProcessor,
            MeterRegistry meterRegistry,
            @Qualifier("graphBuildExecutor") ExecutorService computeExecutor,
            @Qualifier("persistenceExecutor") ExecutorService persistenceExecutor
    ) {
        this.strategyFactory = Objects.requireNonNull(strategyFactory, "strategyFactory must not be null");
        this.potentialMatchComputationProcessor = Objects.requireNonNull(potentialMatchComputationProcessor, "matchProcessor must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.computeExecutor = Objects.requireNonNull(computeExecutor, "computeExecutor must not be null");
        this.persistenceExecutor = Objects.requireNonNull(persistenceExecutor, "persistenceExecutor must not be null");
    }

    @PostConstruct
    private void initialize() {
        this.computeSemaphore = new Semaphore(maxConcurrentBatches, true);
        this.mappingSemaphore = new Semaphore(maxConcurrentMappings, true);
        meterRegistry.gauge("graph_builder_compute_queue", computeExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("graph_builder_compute_active", computeExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
        meterRegistry.gauge("graph_builder_persistence_queue", persistenceExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("graph_builder_persistence_active", persistenceExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
    }

    @PreDestroy
    public void shutdown() {
        this.shutdownInitiated = true;
        try {
            computeExecutor.shutdown();
            persistenceExecutor.shutdown();
            if (!computeExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
                computeExecutor.shutdownNow();
            }
            if (!persistenceExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
                persistenceExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private CompletableFuture<GraphRecords.ChunkResult> processChunk(
            List<Node> nodeChunk,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            int chunkIndex,
            String processingCycleId
    ) {
        List<GraphRecords.PotentialMatch> matches = new ArrayList<>();
        Instant startTime = Instant.now();

        Map<String, Object> context = new HashMap<>();
        context.put("executor", computeExecutor);

        CompletableFuture<List<GraphRecords.PotentialMatch>> matchesFuture = CompletableFuture.supplyAsync(() -> {
            strategy.processBatch(nodeChunk, null, matches, Collections.emptySet(), request, context);
            log.debug("Chunk {} for groupId={} produced {} matches", chunkIndex, request.getGroupId(), matches.size());
            return matches;
        }, computeExecutor);

        Instant rocksStart = Instant.now();
        return matchesFuture.thenComposeAsync(m -> {
                    GraphRecords.ChunkResult chunkResult = new GraphRecords.ChunkResult(Collections.emptySet(), m, chunkIndex, startTime);
                    return potentialMatchComputationProcessor.processChunkMatches(chunkResult, request.getGroupId(), request.getDomainId(), processingCycleId, matchBatchSize)
                            .thenApply(v -> {
                                meterRegistry.timer("graph_builder_rocksdb_persist", "groupId", request.getGroupId().toString(), "processingCycleId", processingCycleId)
                                        .record(Duration.between(rocksStart, Instant.now()));
                                return chunkResult;
                            });
                }, persistenceExecutor)
                .exceptionally(e -> {
                    log.error("Chunk {} failed for groupId={}, processingCycleId={}: {}",
                            chunkIndex, request.getGroupId(), processingCycleId, e.getMessage());
                    throw new CompletionException("Chunk processing failed", e);
                });
    }

    private CompletableFuture<GraphRecords.ChunkResult> processChunkWithRetry(
            List<Node> nodeChunk,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            int chunkIndex,
            String processingCycleId
    ) {
        return retryProcessChunk(nodeChunk, strategy, request, chunkIndex, processingCycleId, 0);
    }

    private CompletableFuture<GraphRecords.ChunkResult> retryProcessChunk(
            List<Node> nodeChunk,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            int chunkIndex,
            String processingCycleId,
            int attempts
    ) {
        if (attempts >= chunkRetryMaxAttempts) {
            log.warn("Chunk {} for groupId={} failed after {} retries, skipping",
                    chunkIndex, request.getGroupId(), chunkRetryMaxAttempts);
            meterRegistry.counter("chunk_processing_skipped", "groupId", request.getGroupId().toString(), "chunkIndex", String.valueOf(chunkIndex)).increment();
            return CompletableFuture.completedFuture(new GraphRecords.ChunkResult(Collections.emptySet(), Collections.emptyList(), chunkIndex, Instant.now()));
        }

        return processChunk(nodeChunk, strategy, request, chunkIndex, processingCycleId)
                .orTimeout(30, TimeUnit.SECONDS)
                .exceptionallyCompose(e -> {
                    log.warn("Chunk {} failed for groupId={}, processingCycleId={} (attempt {}/{}), retrying. Error: {}",
                            chunkIndex, request.getGroupId(), processingCycleId, attempts + 1, chunkRetryMaxAttempts, e.getMessage());
                    meterRegistry.counter("chunk_retry_attempts", "groupId", request.getGroupId().toString()).increment();
                    long delaySeconds = (long) Math.pow(2, attempts) * chunkRetryInitialBackoffSeconds;
                    return CompletableFuture.supplyAsync(() -> null, CompletableFuture.delayedExecutor(delaySeconds, TimeUnit.SECONDS))
                            .thenCompose(__ -> retryProcessChunk(nodeChunk, strategy, request, chunkIndex, processingCycleId, attempts + 1));
                });
    }

    private void handleProcessingError(UUID groupId, int numberOfChunks, String processingCycleId, Throwable e) {
        log.error("Graph processing failed for groupId={}, newChunks={}, processingCycleId={}: {}",
                groupId, numberOfChunks, processingCycleId, e.getMessage());
        meterRegistry.counter("graph_build_errors", "groupId", groupId.toString(), "newChunks", String.valueOf(numberOfChunks),
                "processingCycleId", processingCycleId, "mode", "incremental").increment();
        cleanup(groupId);
        throw new CompletionException("Graph build failed for groupId=" + groupId, e);
    }

    @Override
    public CompletableFuture<GraphRecords.GraphResult> build(List<Node> newNodes, MatchingRequest request) {
        UUID groupId = request.getGroupId();
        UUID domainId = request.getDomainId();
        int page = request.getPage();
        String processingCycleId = request.getProcessingCycleId();

        log.info("Starting graph build for groupId={}, domainId={}, page={}, newNodes={}, processingCycleId={}",
                groupId, domainId, page, newNodes.size(), processingCycleId);

        if (shutdownInitiated) {
            log.warn("Attempted to build graph for groupId={} but builder is shutting down.", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("Graph builder is shutting down."));
        }

        Timer.Sample buildTimer = Timer.start(meterRegistry);
        SymmetricEdgeBuildingStrategy strategy = strategyFactory.createStrategy(request.getWeightFunctionKey(), newNodes);

        CompletableFuture<Void> indexFuture = strategy.indexNodes(newNodes, page);

        List<List<Node>> newNodeChunks = BatchUtils.partition(newNodes, chunkSize)
                .stream()
                .filter(chunk -> !chunk.isEmpty())
                .toList();

        CompletableFuture<GraphRecords.GraphResult> graphResultFuture = new CompletableFuture<>();

        indexFuture.thenRunAsync(() -> {
            try {
                processNodeChunks(newNodeChunks, strategy, request, groupId, domainId, processingCycleId, graphResultFuture);
            } catch (Exception e) {
                handleProcessingError(groupId, newNodeChunks.size(), processingCycleId, e);
            }
        }, computeExecutor).exceptionally(throwable -> {
            log.error("Graph build failed for groupId={}, page={}, newChunks={}, processingCycleId={}: {}",
                    groupId, page, newNodeChunks.size(), processingCycleId, throwable.getMessage());
            if (!graphResultFuture.isDone()) {
                graphResultFuture.completeExceptionally(new InternalServerErrorException("Graph build failed for groupId=" + groupId));
            }
            return null;
        });

        return graphResultFuture.whenComplete((result, throwable) -> {
            buildTimer.stop(meterRegistry.timer("graph_builder_total", "groupId", groupId.toString(), "processingCycleId", processingCycleId,
                    "status", throwable == null ? "success" : "failure"));
            if (throwable != null) {
                cleanup(groupId);
            }
            cleanupGuards.invalidate(groupId);
        });
    }

    private void cleanup(UUID groupId) {
        AtomicBoolean cleanupDone = cleanupGuards.get(groupId, k -> new AtomicBoolean(false));
        if (cleanupDone.compareAndSet(false, true)) {
            try {
                potentialMatchComputationProcessor.cleanup(groupId);
            } catch (Exception e) {
                log.error("Cleanup failed for groupId={}: {}", groupId, e.getMessage());
                throw new CompletionException("Cleanup failed", e);
            }
        }
    }

    private void finalizeGraph(UUID groupId, UUID domainId, String processingCycleId, CompletableFuture<GraphRecords.GraphResult> graphResultFuture) {
        try {
            AutoCloseableStream<Edge> edgeStream = potentialMatchComputationProcessor.streamEdges(groupId, domainId, processingCycleId, topK);
            long edgeCount = edgeStream.getStream().count();
            log.info("Edge stream count for groupId={}: {}", groupId, edgeCount);
            if (edgeCount == 0) {
                log.warn("Empty edge stream for groupId={}", groupId);
                meterRegistry.counter("empty_edge_stream_total", "groupId", groupId.toString()).increment();
            }
            edgeStream = potentialMatchComputationProcessor.streamEdges(groupId, domainId, processingCycleId, topK);
            potentialMatchComputationProcessor.saveFinalMatches(groupId, domainId, processingCycleId, edgeStream, topK)
                    .thenRun(() -> {
                        long finalMatchCount = potentialMatchComputationProcessor.getFinalMatchCount(groupId, domainId, processingCycleId);
                        log.info("Completed graph build: groupId={}, totalMatches={}", groupId, finalMatchCount);
                        meterRegistry.counter("matches_generated_total", "groupId", groupId.toString()).increment(finalMatchCount);
                        potentialMatchComputationProcessor.cleanup(groupId); // Moved after getFinalMatchCount
                        if (!graphResultFuture.isDone()) {
                            graphResultFuture.complete(new GraphRecords.GraphResult(null, Collections.emptyList()));
                        }
                    })
                    .exceptionally(e -> {
                        log.error("Final match saving failed for groupId={}: {}", groupId, e.getMessage());
                        throw new CompletionException("Graph finalization failed", e);
                    });
        } catch (Exception e) {
            log.error("Error during edge streaming for groupId={}: {}", groupId, e.getMessage());
            throw new CompletionException("Graph finalization failed", e);
        }
    }

    private void processNodeChunks(
            List<List<Node>> newNodeChunks,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            UUID groupId,
            UUID domainId,
            String processingCycleId,
            CompletableFuture<GraphRecords.GraphResult> graphResultFuture
    ) {
        List<CompletableFuture<Void>> mappingFutures = Collections.synchronizedList(new ArrayList<>());
        BlockingQueue<GraphRecords.ChunkResult> chunkResults = new LinkedBlockingQueue<>(newNodeChunks.size());
        AtomicInteger chunkIndex = new AtomicInteger(0);
        int totalChunks = newNodeChunks.size();

        ExecutorCompletionService<GraphRecords.ChunkResult> computeService = new ExecutorCompletionService<>(computeExecutor);

        for (int i = 0; i < Math.min(maxConcurrentBatches, totalChunks); i++) {
            submitComputeTask(computeService, newNodeChunks, strategy, request, chunkIndex.getAndIncrement(), groupId, processingCycleId, chunkResults);
        }

        CompletableFuture<Void> computeAndMap = CompletableFuture.runAsync(() -> {
            int processed = 0;
            while (processed < totalChunks && !shutdownInitiated) {
                try {
                    Future<GraphRecords.ChunkResult> future = computeService.poll(300, TimeUnit.SECONDS);
                    if (future != null) {
                        GraphRecords.ChunkResult result = future.get(300, TimeUnit.SECONDS);
                        processed++;
                        if (result != null) {
                            chunkResults.offer(result);
                        }
                    }

                    if (chunkIndex.get() < totalChunks) {
                        submitComputeTask(computeService, newNodeChunks, strategy, request, chunkIndex.getAndIncrement(), groupId, processingCycleId, chunkResults);
                    }
                } catch (InterruptedException e) {
                    log.error("Interrupted processing chunks for groupId={}", groupId);
                    Thread.currentThread().interrupt();
                    break;
                } catch (ExecutionException | TimeoutException e) {
                    log.error("Compute failed for groupId={}: {}", groupId, e.getMessage());
                }
            }
        }, computeExecutor);

        CompletableFuture<Void> mappingPipeline = CompletableFuture.runAsync(() -> {
            while (!shutdownInitiated && (!chunkResults.isEmpty() || chunkIndex.get() < totalChunks)) {
                try {
                    GraphRecords.ChunkResult result = chunkResults.poll(1, TimeUnit.SECONDS);
                    if (result != null) {
                        mappingFutures.add(processMappingAsync(result, groupId, domainId, processingCycleId));
                    }
                } catch (InterruptedException e) {
                    log.error("Interrupted polling chunkResults for groupId={}: {}", groupId, e.getMessage());
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, persistenceExecutor);

        computeAndMap.thenCombine(mappingPipeline, (v1, v2) -> null)
                .thenCompose(v -> CompletableFuture.allOf(mappingFutures.toArray(new CompletableFuture[0])))
                .thenCompose(v -> potentialMatchComputationProcessor.savePendingMatches(groupId, domainId, processingCycleId, matchBatchSize))
                .thenRunAsync(() -> finalizeGraph(groupId, domainId, processingCycleId, graphResultFuture), persistenceExecutor)
                .exceptionally(e -> {
                    handleProcessingError(groupId, totalChunks, processingCycleId, e);
                    return null;
                });
    }

    private void submitComputeTask(
            ExecutorCompletionService<GraphRecords.ChunkResult> computeService,
            List<List<Node>> newNodeChunks,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            int chunkIndex,
            UUID groupId,
            String processingCycleId,
            BlockingQueue<GraphRecords.ChunkResult> chunkResults
    ) {
        if (shutdownInitiated) {
            log.warn("Skipping chunk {} for groupId={} due to shutdown", chunkIndex, groupId);
            return;
        }

        try {
            boolean acquired = computeSemaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timed out acquiring computeSemaphore for chunk {} in groupId={}", chunkIndex, groupId);
                return;
            }

            computeService.submit(() -> {
                try {
                    CompletableFuture<GraphRecords.ChunkResult> future = processChunkWithRetry(
                            newNodeChunks.get(chunkIndex), strategy, request, chunkIndex, processingCycleId
                    ).orTimeout(chunkProcessingTimeoutSeconds, TimeUnit.SECONDS);

                    CompletableFuture<GraphRecords.ChunkResult> resultFuture = new CompletableFuture<>();

                    future.thenAcceptAsync(r -> {
                        if (chunkCounter.incrementAndGet() % 100 == 0 && r != null) {
                            meterRegistry.timer("graph_builder_chunk_compute", "groupId", groupId.toString(), "processingCycleId", processingCycleId)
                                    .record(Duration.between(r.getStartTime(), Instant.now()));
                        }
                        resultFuture.complete(r);
                    }, computeExecutor).exceptionally(ex -> {
                        log.error("Chunk {} failed for groupId={}: {}", chunkIndex, groupId, ex.getMessage());
                        meterRegistry.counter("chunk_processing_errors", "groupId", groupId.toString()).increment();
                        resultFuture.completeExceptionally(ex);
                        return null;
                    });

                    return resultFuture.get(chunkProcessingTimeoutSeconds, TimeUnit.SECONDS);
                } finally {
                    computeSemaphore.release();
                }
            });
        } catch (InterruptedException e) {
            log.error("Interrupted while trying to acquire computeSemaphore for chunk {} in groupId={}", chunkIndex, groupId);
            Thread.currentThread().interrupt();
        }
    }

    private CompletableFuture<Void> processMappingAsync(
            GraphRecords.ChunkResult result,
            UUID groupId,
            UUID domainId,
            String processingCycleId
    ) {
        if (shutdownInitiated) {
            return CompletableFuture.completedFuture(null);
        }

        boolean acquired;
        try {
            acquired = mappingSemaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timed out acquiring mappingSemaphore for groupId={}", groupId);
                return CompletableFuture.completedFuture(null);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while acquiring mappingSemaphore for groupId={}: {}", groupId, e.getMessage());
            Thread.currentThread().interrupt();
            return CompletableFuture.completedFuture(null);
        }

        Instant startTime = Instant.now();
        return potentialMatchComputationProcessor.processChunkMatches(result, groupId, domainId, processingCycleId, matchBatchSize)
                .whenComplete((v, ex) -> {
                    mappingSemaphore.release();
                    if (ex != null) {
                        log.error("Mapping failed for groupId={}: {}", groupId, ex.getMessage());
                        meterRegistry.counter("chunk_processing_errors", "groupId", groupId.toString()).increment();
                    }
                    if (chunkCounter.get() % 100 == 0 && ex == null) {
                        meterRegistry.timer("graph_builder_rocksdb_persist", "groupId", groupId.toString(), "processingCycleId", processingCycleId)
                                .record(Duration.between(startTime, Instant.now()));
                    }
                });
    }
}