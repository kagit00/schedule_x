package com.shedule.x.service;

import com.shedule.x.builder.BipartiteEdgeBuilder;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphFactory;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.processors.PotentialMatchSaver;
import com.shedule.x.processors.GraphStore;
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

    private final BipartiteEdgeBuilder bipartiteEdgeBuilder;
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

    public BipartiteGraphBuilder(
            BipartiteEdgeBuilder bipartiteEdgeBuilder,
            MeterRegistry meterRegistry,
            @Qualifier("graphBuildExecutor") ExecutorService computeExecutor,
            PotentialMatchSaver matchSaver,
            GraphStore graphStore,
            @Qualifier("persistenceExecutor") ExecutorService mappingExecutor
    ) {
        this.bipartiteEdgeBuilder = Objects.requireNonNull(bipartiteEdgeBuilder, "bipartiteEdgeBuilder must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.computeExecutor = Objects.requireNonNull(computeExecutor, "computeExecutor must not be null");
        this.matchSaver = Objects.requireNonNull(matchSaver, "matchSaver must not be null");
        this.graphStore = Objects.requireNonNull(graphStore, "graphStore must not be null");
        this.mappingExecutor = Objects.requireNonNull(mappingExecutor, "mappingExecutor must not be null");
    }

    @PostConstruct
    private void initializeMetrics() {
        meterRegistry.gauge("bipartite_executor_queue", computeExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("bipartite_executor_active", computeExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
        meterRegistry.gauge("bipartite_mapping_queue", mappingExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("bipartite_mapping_active", mappingExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
    }

    @Retry(name = "bipartiteBuild")
    @CircuitBreaker(name = "bipartiteBuild", fallbackMethod = "buildFallback")
    @Override
    public CompletableFuture<GraphRecords.GraphResult> build(List<Node> leftPartition, List<Node> rightPartition, MatchingRequest request) {
        String groupId = request.getGroupId();
        if (groupId == null || groupId.isBlank()) {
            log.error("Invalid groupId in MatchingRequest");
            throw new IllegalArgumentException("groupId must not be null or empty");
        }
        UUID domainId = request.getDomainId();
        int numberOfNodes = leftPartition.size() + rightPartition.size();

        log.info("Building bipartite graph: groupId={}, domainId={}, leftNodes={}, rightNodes={}",
                groupId, domainId, leftPartition.size(), rightPartition.size());
        Timer.Sample buildTimer = Timer.start(meterRegistry);

        List<List<Node>> leftChunks = BatchUtils.partition(leftPartition, chunkSize);
        List<List<Node>> rightChunks = BatchUtils.partition(rightPartition, chunkSize);
        CompletableFuture<GraphRecords.GraphResult> resultFuture = new CompletableFuture<>();
        processChunks(leftChunks, rightChunks, request, groupId, domainId, numberOfNodes, resultFuture);

        return resultFuture.whenComplete((result, throwable) -> {
            buildTimer.stop(meterRegistry.timer("bipartite_build_total", "groupId", groupId, "numberOfNodes", String.valueOf(numberOfNodes)));
            if (throwable != null) {
                log.error("Graph build failed for groupId={}, numberOfNodes={}", groupId, numberOfNodes, throwable);
            }
        });
    }

    public CompletableFuture<GraphRecords.GraphResult> buildFallback(List<Node> leftPartition, List<Node> rightPartition, MatchingRequest request, Throwable t) {
        log.warn("Build fallback for groupId={}", request.getGroupId(), t);
        meterRegistry.counter("bipartite_build_fallbacks", "groupId", request.getGroupId()).increment();
        return CompletableFuture.completedFuture(new GraphRecords.GraphResult(
                GraphFactory.createBipartiteGraph(leftPartition, rightPartition), List.of()));
    }

    private void processChunks(
            List<List<Node>> leftChunks,
            List<List<Node>> rightChunks,
            MatchingRequest request,
            String groupId,
            UUID domainId,
            int numberOfNodes,
            CompletableFuture<GraphRecords.GraphResult> resultFuture
    ) {
        Semaphore computeSemaphore = new Semaphore(maxConcurrentBatches);
        ExecutorCompletionService<GraphRecords.ChunkResult> completionService = new ExecutorCompletionService<>(computeExecutor);
        AtomicInteger chunkIndex = new AtomicInteger(0);
        int totalChunks = Math.min(leftChunks.size(), rightChunks.size());

        computeExecutor.execute(() -> {
            try {
                Iterator<List<Node>> leftIter = leftChunks.iterator();
                Iterator<List<Node>> rightIter = rightChunks.iterator();

                for (int i = 0; i < maxConcurrentBatches && leftIter.hasNext() && rightIter.hasNext(); i++) {
                    boolean acquired = computeSemaphore.tryAcquire(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (!acquired) {
                        log.warn("Timed out acquiring computeSemaphore for groupId={}", groupId);
                        break;
                    }
                    submitChunk(completionService, leftIter.next(), rightIter.next(), request, chunkIndex.getAndIncrement(), computeSemaphore);
                }

                // Process chunks
                List<GraphRecords.PotentialMatch> finalMatches = Collections.synchronizedList(new ArrayList<>());
                for (int processed = 0; processed < totalChunks; processed++) {
                    if (Thread.interrupted()) {
                        log.warn("Chunk processing interrupted for groupId={}", groupId);
                        throw new InterruptedException("Chunk processing interrupted");
                    }

                    GraphRecords.ChunkResult result = completionService.poll(300, TimeUnit.SECONDS).get();

                    meterRegistry.timer("bipartite_chunk_compute", "groupId", groupId, "numberOfNodes", String.valueOf(numberOfNodes))
                            .record(Duration.between(result.getStartTime(), Instant.now()));

                    if (leftIter.hasNext() && rightIter.hasNext()) {
                        boolean acquired = computeSemaphore.tryAcquire(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        if (!acquired) {
                            log.warn("Timed out acquiring computeSemaphore for groupId={} during re-submission", groupId);
                            break;
                        }
                        submitChunk(completionService, leftIter.next(), rightIter.next(), request, chunkIndex.getAndIncrement(), computeSemaphore);
                    }

                    // Persist matches
                    graphStore.persistEdgesAsync(result.getMatches(), groupId, result.getChunkIndex())
                            .orTimeout(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .exceptionally(e -> {
                                log.error("Persist failed for groupId={}, chunkIndex={}", groupId, result.getChunkIndex(), e);
                                meterRegistry.counter("bipartite_persist_errors", "groupId", groupId).increment();
                                return null;
                            });
                }

                // Finalize graph
                Graph graph = GraphFactory.createBipartiteGraph(
                        leftChunks.stream().flatMap(List::stream).collect(Collectors.toList()),
                        rightChunks.stream().flatMap(List::stream).collect(Collectors.toList())
                );

                try (AutoCloseableStream<Edge> edgeStream = graphStore.streamEdges(domainId, groupId)) {
                    edgeStream.forEach(edge -> {
                        graph.addEdge(edge);
                        finalMatches.add(convertToPotentialMatch(edge, groupId, domainId));
                    });
                }

                graphStore.cleanEdges(groupId);

                meterRegistry.counter("matches_generated_total", "groupId", groupId, "numberOfNodes", String.valueOf(numberOfNodes), "mode", "bipartite")
                        .increment(finalMatches.size());

                resultFuture.complete(new GraphRecords.GraphResult(graph, finalMatches));

            } catch (Exception e) {
                handleProcessingError(groupId, numberOfNodes, resultFuture, e);
            } finally {
                while (computeSemaphore.availablePermits() < maxConcurrentBatches) {
                    computeSemaphore.release();
                }
            }
        });
    }

    private GraphRecords.PotentialMatch convertToPotentialMatch(Edge edge, String groupId, UUID domainId) {
        return new GraphRecords.PotentialMatch(
                edge.getFromNode().getReferenceId(),
                edge.getToNode().getReferenceId(),
                edge.getWeight(),
                groupId,
                domainId
        );
    }

    private void submitChunk(
            ExecutorCompletionService<GraphRecords.ChunkResult> completionService,
            List<Node> leftBatch,
            List<Node> rightBatch,
            MatchingRequest request,
            int chunkIndex,
            Semaphore computeSemaphore
    ) {
        if (leftBatch.isEmpty() || rightBatch.isEmpty()) {
            log.debug("Skipping empty chunk for groupId={}, chunkIndex={}", request.getGroupId(), chunkIndex);
            computeSemaphore.release();
            return;
        }

        completionService.submit(() -> {
            try {
                return processBipartiteChunk(leftBatch, rightBatch, request, chunkIndex)
                        .orTimeout(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get();
            } finally {
                computeSemaphore.release();
            }
        });
        meterRegistry.counter("bipartite_futures_pending", "groupId", request.getGroupId()).increment();
    }

    private CompletableFuture<GraphRecords.ChunkResult> processBipartiteChunk(
            List<Node> leftBatch,
            List<Node> rightBatch,
            MatchingRequest request,
            int chunkIndex
    ) {
        Instant start = Instant.now();
        List<GraphRecords.PotentialMatch> matches = new ArrayList<>();
        String groupId = request.getGroupId();

        String weightFunctionKey = request.getWeightFunctionKey();
        if ("flat".equalsIgnoreCase(weightFunctionKey)) {
            matches.addAll(leftBatch.stream()
                    .flatMap(left -> rightBatch.stream()
                            .map(right -> new GraphRecords.PotentialMatch(
                                    left.getReferenceId(), right.getReferenceId(), 1.0, groupId, request.getDomainId())))
                    .collect(Collectors.toList()));
        } else {
            Map<String, Object> context = Map.of("executor", computeExecutor);
            bipartiteEdgeBuilder.processBatch(leftBatch, rightBatch, matches, ConcurrentHashMap.newKeySet(), groupId, request.getDomainId(), context);
        }

        meterRegistry.counter("bipartite_matches_generated", "groupId", groupId).increment(matches.size());
        return CompletableFuture.completedFuture(new GraphRecords.ChunkResult(Set.of(), matches, chunkIndex, start));
    }

    private void handleProcessingError(String groupId, int numberOfNodes, CompletableFuture<GraphRecords.GraphResult> resultFuture, Throwable e) {
        log.error("Graph processing failed for groupId={}, numberOfNodes={}", groupId, numberOfNodes, e);
        meterRegistry.counter("bipartite_build_errors", "groupId", groupId, "numberOfNodes", String.valueOf(numberOfNodes)).increment();
        cleanup(groupId);
        resultFuture.completeExceptionally(new InternalServerErrorException("Graph build failed for groupId=" + groupId));
    }

    private void cleanup(String groupId) {
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