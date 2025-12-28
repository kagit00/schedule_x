package com.shedule.x.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.shedule.x.config.factory.TaskIterator;
import com.shedule.x.dto.ChunkTask;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.processors.PotentialMatchComputationProcessor;
import io.micrometer.core.instrument.Timer;
import com.shedule.x.builder.SymmetricEdgeBuildingStrategy;
import com.shedule.x.config.factory.SymmetricEdgeBuildingStrategyFactory;
import com.shedule.x.dto.MatchingRequest;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;


@Slf4j
@Service
public class SymmetricGraphBuilder implements SymmetricGraphBuilderService {
    private final SymmetricEdgeBuildingStrategyFactory strategyFactory;
    private final PotentialMatchComputationProcessor processor;
    private final MeterRegistry meterRegistry;

    private final ExecutorService computeExecutor;

    private final Cache<UUID, Boolean> cleanupGuards = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.HOURS).build();
    private volatile boolean shutdownInitiated = false;

    @Value("${graph.chunk-size:500}")
    private int chunkSize;

    @Value("${graph.max-concurrent-batches:8}")
    private int maxConcurrentWorkers;

    @Value("${graph.match-batch-size:500}")
    private int matchBatchSize;

    @Value("${graph.top-k:1000}")
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
            meterRegistry.gauge("graph.builder.active", tpe, ThreadPoolExecutor::getActiveCount);
        }
    }

    private CompletableFuture<Void> finalizeBuild(MatchingRequest request, CompletableFuture<GraphRecords.GraphResult> resultFuture) {
        UUID groupId = request.getGroupId();
        log.info("All tasks processed. Finalizing build | groupId={}", groupId);

        return processor.savePendingMatchesAsync(groupId, request.getDomainId(), request.getProcessingCycleId(), matchBatchSize)
                .thenCompose(v ->
                        processor.saveFinalMatches(groupId, request.getDomainId(), request.getProcessingCycleId(), null, topK)
                )
                .thenRun(() -> {
                    long count = processor.getFinalMatchCount(groupId, request.getDomainId(), request.getProcessingCycleId());
                    log.info("Build Complete | groupId={} | Final Count={}", groupId, count);

                    resultFuture.complete(new GraphRecords.GraphResult(null, Collections.emptyList()));
                });
    }

    private void handleFailure(UUID groupId, Throwable ex, Timer.Sample timer, CompletableFuture<?> future) {
        log.error("Build Failed | groupId={}", groupId, ex);
        meterRegistry.counter("graph.build.error").increment();
        if (!future.isDone()) future.completeExceptionally(ex);
    }

    @PreDestroy
    public void shutdown() {
        shutdownInitiated = true;
        computeExecutor.shutdownNow();
    }

    @Override
    public CompletableFuture<GraphRecords.GraphResult> build(
            List<NodeDTO> newNodes,
            MatchingRequest request) {

        UUID groupId = request.getGroupId();
        log.info("Starting symmetric graph build | groupId={} | nodes={}", groupId, newNodes.size());

        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shutting down"));
        }

        Timer.Sample timer = Timer.start(meterRegistry);
        List<List<NodeDTO>> allChunks = BatchUtils.partition(newNodes, chunkSize);
        int numChunks = allChunks.size();


        final TaskIterator taskIterator = new TaskIterator(allChunks);
        final int totalTasks = taskIterator.getTotalTasks();
        final AtomicInteger taskIndexCounter = new AtomicInteger(0);

        SymmetricEdgeBuildingStrategy strategy =
                strategyFactory.createStrategy(request.getWeightFunctionKey(), newNodes);

        CompletableFuture<GraphRecords.GraphResult> resultFuture = new CompletableFuture<>();

        strategy.indexNodes(newNodes, request.getPage())
                .thenRun(() ->
                        startConcurrentWorkers(taskIterator, totalTasks, taskIndexCounter, strategy, request, resultFuture)
                )
                .exceptionally(ex -> {
                    handleFailure(groupId, ex, timer, resultFuture);
                    return null;
                });

        return resultFuture.whenComplete((r, t) -> {
            timer.stop(
                    meterRegistry.timer(
                            "graph.build.duration",
                            "status",
                            t == null ? "success" : "error"
                    )
            );
        });
    }

    private void startConcurrentWorkers(
            TaskIterator taskIterator,
            int totalTasks,
            AtomicInteger taskIndexCounter,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request,
            CompletableFuture<GraphRecords.GraphResult> resultFuture) {

        int workersToStart = Math.min(totalTasks, maxConcurrentWorkers);
        List<CompletableFuture<Void>> workerFutures = new ArrayList<>(workersToStart);

        log.info("Spawning {} workers for {} cross-product tasks | groupId={}",
                workersToStart, totalTasks, request.getGroupId()
        );

        for (int i = 0; i < workersToStart; i++) {
            workerFutures.add(
                    runWorkerChain(taskIterator, totalTasks, taskIndexCounter, strategy, request)
            );
        }

        CompletableFuture.allOf(workerFutures.toArray(new CompletableFuture[0]))
                .thenCompose(v -> finalizeBuild(request, resultFuture))
                .exceptionally(ex -> {
                    handleFailure(request.getGroupId(), ex, null, resultFuture);
                    return null;
                });
    }


    private CompletableFuture<Void> runWorkerChain(
            TaskIterator taskIterator,
            int totalTasks,
            AtomicInteger counter,
            SymmetricEdgeBuildingStrategy strategy,
            MatchingRequest request) {

        if (shutdownInitiated || Thread.currentThread().isInterrupted()) {
            log.warn("Worker chain stopped due to interrupt/shutdown | groupId={}", request.getGroupId());
            return CompletableFuture.completedFuture(null);
        }

        int index = counter.getAndIncrement();
        if (index >= totalTasks) {
            return CompletableFuture.completedFuture(null);
        }

        ChunkTask task = taskIterator.getTaskByIndex(index);

        if (index % 100 == 0) {
            log.info("Progress: {}/{} tasks processed | groupId={}",
                    index, totalTasks, request.getGroupId());
        }

        return CompletableFuture.supplyAsync(() -> {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new CompletionException(new InterruptedException("Worker thread interrupted"));
                    }

                    try {
                        List<GraphRecords.PotentialMatch> matches = new ArrayList<>();
                        Map<String, Object> context = Map.of(
                                "cancellationCheck", (Supplier<Boolean>) () -> Thread.currentThread().isInterrupted()
                        );

                        strategy.processBatch(
                                task.sourceNodes(),
                                task.targetNodes(),
                                matches,
                                Collections.emptySet(),
                                request,
                                context
                        );

                        return new GraphRecords.ChunkResult(
                                Collections.emptySet(),
                                matches,
                                index,
                                Instant.now()
                        );
                    } catch (Exception e) {
                        if (e instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                            throw new CompletionException(new InterruptedException());
                        }
                        throw new CompletionException(e);
                    }
                }, computeExecutor)
                .thenCompose(chunkResult ->
                        processor.processChunkMatches(
                                chunkResult,
                                request.getGroupId(),
                                request.getDomainId(),
                                request.getProcessingCycleId(),
                                matchBatchSize
                        )
                )
                .thenCompose(v ->
                        runWorkerChain(taskIterator, totalTasks, counter, strategy, request)
                );
    }

}