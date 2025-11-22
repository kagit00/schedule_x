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
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Service
public class SymmetricGraphBuilder implements SymmetricGraphBuilderService {

    private final SymmetricEdgeBuildingStrategyFactory strategyFactory;
    private final PotentialMatchComputationProcessor processor;
    private final MeterRegistry meterRegistry;

    // Executors
    private final ExecutorService computeExecutor; // CPU heavy (similarity calc)

    // State management
    private final Cache<UUID, Boolean> cleanupGuards = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS).build();
    private volatile boolean shutdownInitiated = false;

    @Value("${graph.chunk-size:500}")
    private int chunkSize;

    @Value("${graph.max-concurrent-batches:8}")
    private int maxConcurrentWorkers; // Replaces Semaphore

    @Value("${graph.match-batch-size:500}")
    private int matchBatchSize;

    @Value("${graph.top-k:200}")
    private int topK;

    public SymmetricGraphBuilder(
            SymmetricEdgeBuildingStrategyFactory strategyFactory,
            PotentialMatchComputationProcessor processor,
            MeterRegistry meterRegistry,
            @Qualifier("graphBuildExecutor") ExecutorService computeExecutor) {

        this.strategyFactory = strategyFactory;
        this.processor = processor;
        this.meterRegistry = meterRegistry;
        this.computeExecutor = computeExecutor;
    }

    @PostConstruct
    private void initMetrics() {
        if (computeExecutor instanceof ThreadPoolExecutor tpe) {
            //meterRegistry.gauge("graph.builder.queue", tpe, ThreadPoolExecutor::getQueue);
            meterRegistry.gauge("graph.builder.active", tpe, ThreadPoolExecutor::getActiveCount);
        }
    }

    @Override
    public CompletableFuture<GraphRecords.GraphResult> build(List<Node> newNodes, MatchingRequest request) {
        UUID groupId = request.getGroupId();
        log.info("Starting symmetric graph build | groupId={} | nodes={}", groupId, newNodes.size());

        if (shutdownInitiated) return CompletableFuture.failedFuture(new IllegalStateException("Shutting down"));

        Timer.Sample timer = Timer.start(meterRegistry);

        // 1. Partition Data
        List<List<Node>> allChunks = BatchUtils.partition(newNodes, chunkSize);
        AtomicInteger chunkIndexCounter = new AtomicInteger(0);

        // 2. Initialize Strategy
        SymmetricEdgeBuildingStrategy strategy = strategyFactory.createStrategy(request.getWeightFunctionKey(), newNodes);

        CompletableFuture<GraphRecords.GraphResult> resultFuture = new CompletableFuture<>();

        // 3. Index Nodes (Blocking Phase) -> Then Start Workers
        strategy.indexNodes(newNodes, request.getPage())
                .thenRun(() -> startConcurrentWorkers(allChunks, chunkIndexCounter, strategy, request, resultFuture))
                .exceptionally(ex -> {
                    handleFailure(groupId, ex, timer, resultFuture);
                    return null;
                });

        return resultFuture.whenComplete((r, t) -> {
            timer.stop(meterRegistry.timer("graph.build.duration", "status", t == null ? "success" : "error"));
        });
    }

    /**
     * Starts 'maxConcurrentWorkers' parallel chains.
     * Each chain processes one chunk, then picks the next available chunk index.
     */
    private void startConcurrentWorkers(
            List<List<Node>> allChunks,
            AtomicInteger chunkIndexCounter,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            CompletableFuture<GraphRecords.GraphResult> resultFuture) {

        int totalChunks = allChunks.size();
        int workersToStart = Math.min(totalChunks, maxConcurrentWorkers);
        List<CompletableFuture<Void>> workerFutures = new ArrayList<>();

        log.info("Spawning {} workers for {} chunks | groupId={}", workersToStart, totalChunks, request.getGroupId());

        for (int i = 0; i < workersToStart; i++) {
            workerFutures.add(runWorkerChain(allChunks, chunkIndexCounter, strategy, request));
        }

        // Wait for all workers to finish
        CompletableFuture.allOf(workerFutures.toArray(new CompletableFuture[0]))
                .thenCompose(v -> finalizeBuild(request, resultFuture))
                .exceptionally(ex -> {
                    handleFailure(request.getGroupId(), ex, null, resultFuture);
                    return null;
                });
    }

    /**
     * recursive chain: Get Index -> Process -> Ingest -> Recurse
     */
    private CompletableFuture<Void> runWorkerChain(
            List<List<Node>> allChunks,
            AtomicInteger counter,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request) {

        if (shutdownInitiated || Thread.currentThread().isInterrupted()) {
            return CompletableFuture.completedFuture(null);
        }

        // 1. Pick next chunk atomically
        int myIndex = counter.getAndIncrement();
        if (myIndex >= allChunks.size()) {
            return CompletableFuture.completedFuture(null); // No more work
        }

        List<Node> chunk = allChunks.get(myIndex);

        // 2. Compute Matches (CPU Bound)
        return CompletableFuture.supplyAsync(() -> {
                    try {
                        // Ensure strategies are thread-safe or locally instantiated if not
                        List<GraphRecords.PotentialMatch> matches = new ArrayList<>();
                        strategy.processBatch(chunk, null, matches, Collections.emptySet(), request, Map.of());

                        return new GraphRecords.ChunkResult(Collections.emptySet(), matches, myIndex, Instant.now());
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, computeExecutor)
                // 3. Send to Processor (Memory/Disk Queue)
                .thenCompose(chunkResult ->
                        processor.processChunkMatches(chunkResult, request.getGroupId(), request.getDomainId(), request.getProcessingCycleId(), matchBatchSize)
                )
                // 4. Recurse (Pick next chunk)
                .thenCompose(v -> runWorkerChain(allChunks, counter, strategy, request));
    }

    private CompletableFuture<Void> finalizeBuild(MatchingRequest request, CompletableFuture<GraphRecords.GraphResult> resultFuture) {
        UUID groupId = request.getGroupId();
        log.info("All chunks processed. Finalizing build | groupId={}", groupId);

        // 1. Save Pending (Flush Queue) - The Processor method we added earlier
        return processor.savePendingMatchesAsync(groupId, request.getDomainId(), request.getProcessingCycleId(), matchBatchSize)
                .thenCompose(v ->
                        // 2. Final Save (Streaming)
                        processor.saveFinalMatches(groupId, request.getDomainId(), request.getProcessingCycleId(), null, topK)
                )
                .thenRun(() -> {
                    long count = processor.getFinalMatchCount(groupId, request.getDomainId(), request.getProcessingCycleId());
                    log.info("Build Complete | groupId={} | Final Count={}", groupId, count);

                    // 3. Cleanup
                    performCleanup(groupId);
                    resultFuture.complete(new GraphRecords.GraphResult(null, Collections.emptyList()));
                });
    }

    private void handleFailure(UUID groupId, Throwable ex, Timer.Sample timer, CompletableFuture<?> future) {
        log.error("Build Failed | groupId={}", groupId, ex);
        meterRegistry.counter("graph.build.error").increment();
        performCleanup(groupId);
        if (!future.isDone()) future.completeExceptionally(ex);
    }

    private void performCleanup(UUID groupId) {
        // Ensure cleanup runs only once per group execution
        if (Boolean.TRUE.equals(cleanupGuards.asMap().putIfAbsent(groupId, true))) {
            return;
        }
        try {
            processor.cleanup(groupId);
        } catch (Exception e) {
            log.warn("Cleanup warning | groupId={}", groupId, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        shutdownInitiated = true;
        computeExecutor.shutdownNow();
    }
}