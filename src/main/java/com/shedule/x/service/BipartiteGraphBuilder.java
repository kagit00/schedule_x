package com.shedule.x.service;

import com.shedule.x.builder.BipartiteEdgeBuildingStrategy;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphFactory;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.dto.Graph;
import com.shedule.x.processors.GraphStore;
import com.shedule.x.processors.PotentialMatchSaver;
import com.shedule.x.utils.db.BatchUtils;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BipartiteGraphBuilder implements BipartiteGraphBuilderService {
    private static final long CHUNK_TIMEOUT_SECONDS = 30;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;

    private final BipartiteEdgeBuildingStrategy bipartiteEdgeBuildingStrategy;
    private final ExecutorService mappingExecutor;
    private final MeterRegistry meterRegistry;
    private final ExecutorService computeExecutor;
    private final PotentialMatchSaver matchSaver;
    private final GraphStore graphStore;

    @Value("${graph.chunk-size:500}")
    private int chunkSize;

    @Value("${graph.max-concurrent-batches:4}")
    private int maxConcurrentBatches;

    @Value("${graph.top-k:100}")
    private int topK;

    private final AtomicInteger bipartiteFuturesPending = new AtomicInteger(0);

    public BipartiteGraphBuilder(
            BipartiteEdgeBuildingStrategy bipartiteEdgeBuildingStrategy,
            MeterRegistry meterRegistry,
            @Qualifier("graphBuildExecutor") ExecutorService computeExecutor,
            PotentialMatchSaver matchSaver,
            GraphStore graphStoreImp,
            @Qualifier("persistenceExecutor") ExecutorService mappingExecutor) {
        this.bipartiteEdgeBuildingStrategy = Objects.requireNonNull(bipartiteEdgeBuildingStrategy,
                "bipartiteEdgeBuilder must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.computeExecutor = Objects.requireNonNull(computeExecutor, "computeExecutor must not be null");
        this.matchSaver = Objects.requireNonNull(matchSaver, "matchSaver must not be null");
        this.graphStore = Objects.requireNonNull(graphStoreImp, "graphStore must not be null");
        this.mappingExecutor = Objects.requireNonNull(mappingExecutor, "mappingExecutor must not be null");
    }


    @PostConstruct
    private void initializeMetrics() {
        if (computeExecutor instanceof ThreadPoolExecutor) {
            meterRegistry.gauge("bipartite_executor_queue", computeExecutor,
                    exec -> ((ThreadPoolExecutor) exec).getQueue().size());
            meterRegistry.gauge("bipartite_executor_active", computeExecutor,
                    exec -> ((ThreadPoolExecutor) exec).getActiveCount());
        } else {
            log.warn("Compute executor is not a ThreadPoolExecutor. Cannot register queue and active count gauges.");
        }
        if (mappingExecutor instanceof ThreadPoolExecutor) {
            meterRegistry.gauge("bipartite_mapping_queue", mappingExecutor,
                    exec -> ((ThreadPoolExecutor) exec).getQueue().size());
            meterRegistry.gauge("bipartite_mapping_active", mappingExecutor,
                    exec -> ((ThreadPoolExecutor) exec).getActiveCount());
        } else {
            log.warn("Mapping executor is not a ThreadPoolExecutor. Cannot register queue and active count gauges.");
        }

        meterRegistry.gauge("bipartite_futures_pending", bipartiteFuturesPending);
        meterRegistry.counter("bipartite_futures_completed");
    }


    @Retry(name = "bipartiteBuild")
    @CircuitBreaker(name = "bipartiteBuild", fallbackMethod = "buildFallback")
    @Override
    public CompletableFuture<GraphRecords.GraphResult> build(List<NodeDTO> leftPartition, List<NodeDTO> rightPartition,
                                                             MatchingRequest request) {
        UUID groupId = request.getGroupId();
        if (groupId == null) {
            log.error("Invalid groupId in MatchingRequest");
            throw new IllegalArgumentException("groupId must not be null or empty");
        }
        UUID domainId = request.getDomainId();

        if (leftPartition == null || rightPartition == null) {
            log.error("Left or right partition is null for groupId={}", groupId);
            throw new IllegalArgumentException("leftPartition and rightPartition must not be null");
        }

        if (leftPartition.isEmpty() || rightPartition.isEmpty()) {
            log.info("Empty left or right partition for groupId={}, returning empty graph", groupId);
            Graph graph = GraphFactory.createBipartiteGraph(leftPartition, rightPartition);
            return CompletableFuture.completedFuture(new GraphRecords.GraphResult(graph, List.of()));
        }

        int numberOfNodes = leftPartition.size() + rightPartition.size();

        log.info("Building bipartite graph: groupId={}, domainId={}, leftNodes={}, rightNodes={}",
                groupId, domainId, leftPartition.size(), rightPartition.size());
        Timer.Sample buildTimer = Timer.start(meterRegistry);

        List<List<NodeDTO>> leftChunks = BatchUtils.partition(leftPartition, chunkSize);
        List<List<NodeDTO>> rightChunks = BatchUtils.partition(rightPartition, chunkSize);
        CompletableFuture<GraphRecords.GraphResult> resultFuture = new CompletableFuture<>();
        processChunks(leftChunks, rightChunks, request, groupId, domainId, numberOfNodes, resultFuture);

        return resultFuture.whenComplete((result, throwable) -> {
            buildTimer.stop(meterRegistry.timer("bipartite_build_total", "groupId", groupId.toString(), "numberOfNodes",
                    String.valueOf(numberOfNodes)));
            if (throwable != null) {
                log.error("Graph build failed for groupId={}, numberOfNodes={}", groupId, numberOfNodes, throwable);
            }
        });
    }

    public CompletableFuture<GraphRecords.GraphResult> buildFallback(List<NodeDTO> leftPartition,
            List<NodeDTO> rightPartition, MatchingRequest request, Throwable t) {
        log.warn("Build fallback for groupId={}", request.getGroupId(), t);
        meterRegistry.counter("bipartite_build_fallbacks", "groupId", request.getGroupId().toString()).increment();
        return CompletableFuture.completedFuture(new GraphRecords.GraphResult(
                GraphFactory.createBipartiteGraph(leftPartition, rightPartition), List.of()));
    }

    private void processChunks(
            List<List<NodeDTO>> leftChunks,
            List<List<NodeDTO>> rightChunks,
            MatchingRequest request,
            UUID groupId,
            UUID domainId,
            int numberOfNodes,
            CompletableFuture<GraphRecords.GraphResult> resultFuture) {
        Semaphore computeSemaphore = new Semaphore(maxConcurrentBatches);
        ExecutorCompletionService<GraphRecords.ChunkResult> completionService = new ExecutorCompletionService<>(
                computeExecutor);

        final int leftChunkCount = leftChunks.size();
        final int rightChunkCount = rightChunks.size();
        final long totalTasks = (long) leftChunkCount * Math.max(1, rightChunkCount);

        AtomicInteger nextLeftIndex = new AtomicInteger(0);
        AtomicInteger nextRightIndex = new AtomicInteger(0);
        AtomicInteger chunkIndex = new AtomicInteger(0);

        List<Future<GraphRecords.ChunkResult>> submittedTaskFutures = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> persistenceFutures = Collections.synchronizedList(new ArrayList<>());
        List<GraphRecords.PotentialMatch> finalMatches = Collections.synchronizedList(new ArrayList<>());

        computeExecutor.execute(() -> {
            try {
                for (int s = 0; s < Math.min(totalTasks, maxConcurrentBatches); s++) {
                    PairIndices p = nextPair(leftChunkCount, rightChunkCount, nextLeftIndex, nextRightIndex);
                    if (p == null)
                        break;
                    boolean acquired = computeSemaphore.tryAcquire(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (!acquired) {
                        throw new InternalServerErrorException(
                                "Timed out acquiring compute semaphore while submitting initial chunks for groupId="
                                        + groupId);
                    }
                    Future<GraphRecords.ChunkResult> fut = submitChunk(completionService, leftChunks.get(p.left),
                            rightChunks.get(p.right), request, chunkIndex.getAndIncrement(), computeSemaphore);
                    submittedTaskFutures.add(fut);
                    bipartiteFuturesPending.incrementAndGet();
                }

                long processed = 0;
                while (processed < totalTasks) {
                    if (Thread.currentThread().isInterrupted()) {
                        log.warn("Chunk processing interrupted for groupId={}", groupId);
                        throw new InterruptedException("Chunk processing interrupted");
                    }

                    Future<GraphRecords.ChunkResult> chunkFuture = completionService.take();
                    submittedTaskFutures.remove(chunkFuture);
                    bipartiteFuturesPending.decrementAndGet();
                    GraphRecords.ChunkResult result = null;
                    try {
                        result = chunkFuture.get();
                    } catch (ExecutionException ee) {
                        throw ee;
                    }
                    processed++;

                    meterRegistry
                            .timer("bipartite_chunk_compute", "groupId", groupId.toString(), "numberOfNodes",
                                    String.valueOf(numberOfNodes))
                            .record(Duration.between(result.getStartTime(), Instant.now()));

                    PairIndices next = nextPair(leftChunkCount, rightChunkCount, nextLeftIndex, nextRightIndex);
                    if (next != null) {
                        boolean acquired = computeSemaphore.tryAcquire(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        if (!acquired) {
                            throw new InternalServerErrorException(
                                    "Timed out acquiring compute semaphore while submitting follow-up chunks for groupId="
                                            + groupId);
                        }
                        Future<GraphRecords.ChunkResult> fut = submitChunk(completionService, leftChunks.get(next.left),
                                rightChunks.get(next.right), request, chunkIndex.getAndIncrement(), computeSemaphore);
                        submittedTaskFutures.add(fut);
                        bipartiteFuturesPending.incrementAndGet();
                    }

                    GraphRecords.ChunkResult finalResult = result;
                    CompletableFuture<Void> persistFuture = graphStore
                            .persistEdgesAsync(result.getMatches(), groupId, result.getChunkIndex())
                            .orTimeout(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .exceptionally(e -> {
                                log.error("Persist failed for groupId={}, chunkIndex={}", groupId,
                                        finalResult.getChunkIndex(), e);
                                meterRegistry.counter("bipartite_persist_errors", "groupId", groupId.toString())
                                        .increment();
                                return null;
                            });
                    persistenceFutures.add(persistFuture);

                    meterRegistry.counter("bipartite_futures_completed").increment();
                }

                long dynamicTimeoutSeconds = CHUNK_TIMEOUT_SECONDS
                        * Math.max(2, (int) ((totalTasks + maxConcurrentBatches - 1) / maxConcurrentBatches));
                log.info("Waiting for {} persistence operations to complete for groupId={} with timeout {}s",
                        persistenceFutures.size(), groupId, dynamicTimeoutSeconds);
                CompletableFuture.allOf(persistenceFutures.toArray(new CompletableFuture[0]))
                        .get(dynamicTimeoutSeconds, TimeUnit.SECONDS);

                Graph graph = GraphFactory.createBipartiteGraph(
                        leftChunks.stream().flatMap(List::stream).collect(Collectors.toList()),
                        rightChunks.stream().flatMap(List::stream).collect(Collectors.toList()));

                try (AutoCloseableStream<EdgeDTO> edgeStream = graphStore.streamEdges(domainId, groupId)) {
                    edgeStream.forEach(edge -> {
                        graph.addEdge(edge);
                        finalMatches.add(convertToPotentialMatch(edge, groupId, domainId));
                    });
                }

                graphStore.cleanEdges(groupId);

                meterRegistry
                        .counter("matches_generated_total", "groupId", groupId.toString(), "numberOfNodes",
                                String.valueOf(numberOfNodes), "mode", "bipartite")
                        .increment(finalMatches.size());

                resultFuture.complete(new GraphRecords.GraphResult(graph, finalMatches));
            } catch (Throwable e) {
                submittedTaskFutures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Exception ex) {
                        log.debug("Failed to cancel task future", ex);
                    }
                });
                handleProcessingError(groupId, numberOfNodes, resultFuture, e);
            }
        });
    }

    private static class PairIndices {
        final int left;
        final int right;

        PairIndices(int l, int r) {
            this.left = l;
            this.right = r;
        }
    }

    private PairIndices nextPair(int leftChunkCount, int rightChunkCount, AtomicInteger nextLeftIndex,
            AtomicInteger nextRightIndex) {
        synchronized (nextLeftIndex) {
            int i = nextLeftIndex.get();
            int j = nextRightIndex.get();
            if (i >= leftChunkCount)
                return null;
            PairIndices p = new PairIndices(i, j);
            int newJ = j + 1;
            int newI = i;
            if (newJ >= rightChunkCount) {
                newJ = 0;
                newI = i + 1;
            }
            nextRightIndex.set(newJ);
            nextLeftIndex.set(newI);
            return p;
        }
    }

    private Future<GraphRecords.ChunkResult> submitChunk(
            ExecutorCompletionService<GraphRecords.ChunkResult> completionService,
            List<NodeDTO> leftBatch,
            List<NodeDTO> rightBatch,
            MatchingRequest request,
            int chunkIndex,
            Semaphore computeSemaphore) {
        if (leftBatch.isEmpty() || rightBatch.isEmpty()) {
            log.debug("Skipping empty chunk for groupId={}, chunkIndex={}", request.getGroupId(), chunkIndex);
            computeSemaphore.release();
            return CompletableFuture.completedFuture(null);
        }

        Future<GraphRecords.ChunkResult> future = completionService.submit(() -> {
            try {
                return processBipartiteChunk(leftBatch, rightBatch, request, chunkIndex)
                        .orTimeout(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get();
            } finally {
                computeSemaphore.release();
            }
        });
        bipartiteFuturesPending.incrementAndGet();
        return future;
    }


    private GraphRecords.PotentialMatch convertToPotentialMatch(EdgeDTO edge, UUID groupId, UUID domainId) {
        return new GraphRecords.PotentialMatch(
                edge.getFromNodeHash(),
                edge.getToNodeHash(),
                edge.getScore(),
                groupId,
                domainId);
    }


    private CompletableFuture<GraphRecords.ChunkResult> processBipartiteChunk(
            List<NodeDTO> leftBatch,
            List<NodeDTO> rightBatch,
            MatchingRequest request,
            int chunkIndex) {
        Instant start = Instant.now();
        List<GraphRecords.PotentialMatch> matches = new ArrayList<>();
        UUID groupId = request.getGroupId();

        String weightFunctionKey = request.getWeightFunctionKey();
        if ("flat".equalsIgnoreCase(weightFunctionKey)) {
            matches.addAll(leftBatch.stream()
                    .flatMap(left -> rightBatch.stream()
                            .map(right -> new GraphRecords.PotentialMatch(
                                    left.getReferenceId(), right.getReferenceId(), 1.0, groupId,
                                    request.getDomainId())))
                    .toList());
        } else {
            Map<String, Object> context = Map.of("executor", computeExecutor);
            bipartiteEdgeBuildingStrategy.processBatch(leftBatch, rightBatch, matches, ConcurrentHashMap.newKeySet(), groupId,
                    request.getDomainId(), context);
        }

        meterRegistry.counter("bipartite_matches_generated", "groupId", groupId.toString()).increment(matches.size());
        return CompletableFuture.completedFuture(new GraphRecords.ChunkResult(Set.of(), matches, chunkIndex, start));
    }


    private void handleProcessingError(UUID groupId, int numberOfNodes,
            CompletableFuture<GraphRecords.GraphResult> resultFuture, Throwable e) {
        log.error("Graph processing failed for groupId={}, numberOfNodes={}", groupId, numberOfNodes, e);
        meterRegistry.counter("bipartite_build_errors", "groupId", groupId.toString(), "numberOfNodes",
                String.valueOf(numberOfNodes)).increment();
        cleanup(groupId);
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        resultFuture.completeExceptionally(
                new InternalServerErrorException("Graph build failed for groupId=" + groupId + ": " + e.getMessage()));
    }


    private void cleanup(UUID groupId) {
        try {
            graphStore.cleanEdges(groupId);
            matchSaver.deleteByGroupId(groupId).exceptionally(ex -> {
                log.error("Failed to delete matches for groupId={}", groupId, ex);
                return null;
            });
        } catch (Exception e) {
            log.error("Cleanup failed for groupId={}", groupId, e);
            throw new InternalServerErrorException("Cleanup failed");
        }
    }


    @PreDestroy
    public void shutdown() {
        try {
            computeExecutor.shutdown();
            mappingExecutor.shutdown();
            if (!computeExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
                computeExecutor.shutdownNow();
            }
            if (!mappingExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
                mappingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
}
