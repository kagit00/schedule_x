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

    private static final long CHUNK_TIMEOUT_SECONDS = 1800L;
    private static final long CHUNK_HARD_KILL_SECONDS = 2700L;

    private final SymmetricEdgeBuildingStrategyFactory strategyFactory;
    private final PotentialMatchComputationProcessor potentialMatchComputationProcessor;
    private final MeterRegistry meterRegistry;
    private final ExecutorService computeExecutor;
    private final ExecutorService persistenceExecutor;
    private final ScheduledExecutorService watchdogExecutor;

    private final Semaphore computeSemaphore;
    private final Semaphore mappingSemaphore;

    private final Cache<UUID, AtomicBoolean> cleanupGuards = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();

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

    public SymmetricGraphBuilder(
            SymmetricEdgeBuildingStrategyFactory strategyFactory,
            PotentialMatchComputationProcessor potentialMatchComputationProcessor,
            MeterRegistry meterRegistry,
            @Qualifier("graphBuildExecutor") ExecutorService computeExecutor,
            @Qualifier("persistenceExecutor") ExecutorService persistenceExecutor,
            @Qualifier("watchdogExecutor") ScheduledExecutorService watchdogExecutor,
            @Value("${graph.max-concurrent-batches:3}") int maxConcurrentBatches,
            @Value("${graph.max-concurrent-mappings:6}") int maxConcurrentMappings) {

        this.strategyFactory = strategyFactory;
        this.potentialMatchComputationProcessor = potentialMatchComputationProcessor;
        this.meterRegistry = meterRegistry;
        this.computeExecutor = computeExecutor;
        this.persistenceExecutor = persistenceExecutor;
        this.watchdogExecutor = watchdogExecutor;

        this.computeSemaphore = new Semaphore(maxConcurrentBatches, true);
        this.mappingSemaphore = new Semaphore(maxConcurrentMappings, true);
    }

    @PostConstruct
    private void initMetrics() {
        meterRegistry.gauge("graph_builder_compute_queue", computeExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("graph_builder_compute_active", computeExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
        meterRegistry.gauge("graph_builder_persistence_queue", persistenceExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("graph_builder_persistence_active", persistenceExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
    }

    @PreDestroy
    public void shutdown() {
        shutdownInitiated = true;
        computeExecutor.shutdownNow();
        persistenceExecutor.shutdownNow();
    }

    @Override
    public CompletableFuture<GraphRecords.GraphResult> build(List<Node> newNodes, MatchingRequest request) {
        UUID groupId = request.getGroupId();
        UUID domainId = request.getDomainId();
        String processingCycleId = request.getProcessingCycleId();

        log.info("Starting symmetric graph build | groupId={} | nodes={} | cycle={}", groupId, newNodes.size(), processingCycleId);

        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("Builder shutting down"));
        }

        Timer.Sample timer = Timer.start(meterRegistry);
        SymmetricEdgeBuildingStrategy strategy = strategyFactory.createStrategy(request.getWeightFunctionKey(), newNodes);

        List<List<Node>> chunks = BatchUtils.partition(newNodes, chunkSize)
                .stream().filter(c -> !c.isEmpty()).toList();

        CompletableFuture<GraphRecords.GraphResult> resultFuture = new CompletableFuture<>();

        strategy.indexNodes(newNodes, request.getPage())
                .thenRun(() -> processChunksWithCancellation(chunks, strategy, request, resultFuture))
                .exceptionally(ex -> {
                    failBuild(groupId, processingCycleId, ex, timer, resultFuture);
                    return null;
                });

        return resultFuture.whenComplete((r, t) -> {
            timer.stop(meterRegistry.timer("graph_builder_total", "groupId", groupId.toString(),
                    "status", t == null ? "success" : "failure"));
            cleanupGuards.invalidate(groupId);
        });
    }

    private void processChunksWithCancellation(
            List<List<Node>> chunks,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            CompletableFuture<GraphRecords.GraphResult> resultFuture) {

        UUID groupId = request.getGroupId();
        String cycleId = request.getProcessingCycleId();

        List<CompletableFuture<Void>> allFutures = new CopyOnWriteArrayList<>();
        AtomicInteger completed = new AtomicInteger(0);
        int total = chunks.size();

        for (int i = 0; i < chunks.size(); i++) {
            List<Node> chunk = chunks.get(i);
            int chunkIdx = i;

            if (Thread.interrupted() || shutdownInitiated) break;

            CompletableFuture<Void> chunkFuture = submitCancellableChunk(
                    chunk, strategy, request, chunkIdx, cycleId, allFutures.size());

            allFutures.add(chunkFuture);

            chunkFuture.handle((v, ex) -> {
                if (ex != null) {
                    log.error("Chunk {} failed → cancelling entire build | groupId={}", chunkIdx, groupId, ex);
                    allFutures.forEach(f -> f.completeExceptionally(new RuntimeException("Sibling chunk failed")));
                    resultFuture.completeExceptionally(ex);
                } else if (completed.incrementAndGet() == total) {
                    finalizeBuild(groupId, request.getDomainId(), cycleId, resultFuture);
                }
                return null;
            });
        }

        if (chunks.isEmpty()) {
            finalizeBuild(groupId, request.getDomainId(), cycleId, resultFuture);
        }
    }

    private void finalizeBuild(UUID groupId, UUID domainId, String cycleId, CompletableFuture<GraphRecords.GraphResult> resultFuture) {
        potentialMatchComputationProcessor.saveFinalMatches(groupId, domainId, cycleId, null, topK)
                .thenRun(() -> {
                    long count = potentialMatchComputationProcessor.getFinalMatchCount(groupId, domainId, cycleId);
                    log.info("Graph build completed | groupId={} | finalMatches={}", groupId, count);
                    cleanup(groupId);
                    resultFuture.complete(new GraphRecords.GraphResult(null, Collections.emptyList()));
                })
                .exceptionally(ex -> {
                    log.error("Finalization failed | groupId={}", groupId, ex);
                    cleanup(groupId);
                    resultFuture.completeExceptionally(ex);
                    return null;
                });
    }

    private void cleanup(UUID groupId) {
        AtomicBoolean done = cleanupGuards.get(groupId, k -> new AtomicBoolean(false));
        if (done.compareAndSet(false, true)) {
            try {
                potentialMatchComputationProcessor.cleanup(groupId);
            } catch (Exception e) {
                log.error("Cleanup failed for groupId={}", groupId, e);
            }
        }
    }

    private void failBuild(UUID groupId, String cycleId, Throwable t, Timer.Sample timer, CompletableFuture<?> future) {
        log.error("Graph build failed | groupId={} | cycle={}", groupId, cycleId, t);
        meterRegistry.counter("graph_build_errors", "groupId", groupId.toString()).increment();
        cleanup(groupId);
        if (!future.isDone()) {
            future.completeExceptionally(t instanceof RuntimeException re ? re : new RuntimeException(t));
        }
    }

    private CompletableFuture<Void> submitCancellableChunk(
            List<Node> chunk,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            int chunkIdx,
            String cycleId,
            int taskId) {

        UUID groupId = request.getGroupId();

        return acquireSemaphoreAsync(computeSemaphore)
                .thenCompose(v -> {
                    // Start computation
                    CompletableFuture<GraphRecords.ChunkResult> computation =
                            CompletableFuture.supplyAsync(() -> {
                                        Thread.currentThread().setName("chunk-" + chunkIdx + "-" + groupId);
                                        List<GraphRecords.PotentialMatch> matches = new ArrayList<>();
                                        strategy.processBatch(chunk, null, matches, Collections.emptySet(), request, Map.of());
                                        return new GraphRecords.ChunkResult(Collections.emptySet(), matches, chunkIdx, Instant.now());
                                    }, computeExecutor)
                                    .orTimeout(CHUNK_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                    return computation.whenComplete((r, e) -> computeSemaphore.release());
                })
                .thenCompose(result -> acquireSemaphoreAsync(mappingSemaphore)
                        .thenCompose(v -> {
                            CompletableFuture<Void> processing = potentialMatchComputationProcessor
                                    .processChunkMatches(result, groupId, request.getDomainId(), cycleId, matchBatchSize)
                                    .orTimeout(600, TimeUnit.SECONDS);

                            return processing.whenComplete((r, e) -> mappingSemaphore.release());
                        }))
                .orTimeout(CHUNK_HARD_KILL_SECONDS, TimeUnit.SECONDS)
                .whenComplete((v, t) -> {
                    if (t instanceof TimeoutException) {
                        log.error("HARD KILL chunk {} | groupId={}", chunkIdx, groupId);
                        meterRegistry.counter("graph_chunk_hard_kill", "groupId", groupId.toString()).increment();
                    }
                });
    }

    private CompletableFuture<Void> acquireSemaphoreAsync(Semaphore semaphore) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                if (!semaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    future.completeExceptionally(new TimeoutException("Semaphore acquisition timeout"));
                } else {
                    future.complete(null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            }
        }); // Uses ForkJoinPool.commonPool() - doesn't block your executors
        return future;
    }
}