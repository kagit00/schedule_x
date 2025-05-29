package com.shedule.x.processors;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.models.Node;
import com.shedule.x.partition.PartitionStrategy;
import com.shedule.x.service.BipartiteGraphBuilderService;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.service.SymmetricGraphBuilderService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;


@Slf4j
@Component
public class GraphPreProcessor {
    private static final long BUILD_TIMEOUT_MINUTES = 15;

    private final SymmetricGraphBuilderService symmetricGraphBuilder;
    private final BipartiteGraphBuilderService bipartiteGraphBuilder;
    private final MeterRegistry meterRegistry;
    private final ExecutorService executor;
    private final ScheduledExecutorService watchdogExecutor;
    private final PartitionStrategy partitionStrategy;
    private final Semaphore buildSemaphore;

    public GraphPreProcessor(
            SymmetricGraphBuilderService symmetricGraphBuilder,
            BipartiteGraphBuilderService bipartiteGraphBuilder,
            MeterRegistry meterRegistry,
            @Qualifier("graphBuildExecutor") ExecutorService executor,
            @Qualifier("watchdogExecutor") ScheduledExecutorService watchdogExecutor,
            @Qualifier("metadataBasedPartitioningStrategy") PartitionStrategy partitionStrategy,
            @Value("${graph.max-concurrent-builds:2}") int maxConcurrentBuilds
    ) {
        this.symmetricGraphBuilder = Objects.requireNonNull(symmetricGraphBuilder, "symmetricGraphBuilder must not be null");
        this.bipartiteGraphBuilder = Objects.requireNonNull(bipartiteGraphBuilder, "bipartiteGraphBuilder must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.watchdogExecutor = Objects.requireNonNull(watchdogExecutor, "watchdogExecutor must not be null");
        this.partitionStrategy = Objects.requireNonNull(partitionStrategy, "partitionStrategy must not be null");
        this.buildSemaphore = new Semaphore(maxConcurrentBuilds, true);
        meterRegistry.gauge("graph_build_queue_length", buildSemaphore, Semaphore::getQueueLength);
    }

    public MatchType inferMatchType(List<Node> leftNodes, List<Node> rightNodes) {
        Set<String> firstTypes = leftNodes.stream().map(Node::getType).filter(Objects::nonNull).collect(Collectors.toSet());
        Set<String> secondTypes = rightNodes.stream().map(Node::getType).filter(Objects::nonNull).collect(Collectors.toSet());
        if (firstTypes.isEmpty() || secondTypes.isEmpty()) {
            log.warn("Empty or null node types detected; defaulting to SYMMETRIC");
            return MatchType.SYMMETRIC;
        }
        return firstTypes.equals(secondTypes) ? MatchType.SYMMETRIC : MatchType.BIPARTITE;
    }

    public CompletableFuture<GraphRecords.GraphResult> buildGraph(List<Node> nodes, MatchingRequest request) {
        String groupId = request.getGroupId();
        String mode = "batch";
        Timer.Sample sample = Timer.start(meterRegistry);
        Timer.Sample buildSample = Timer.start(meterRegistry);

        return acquireAndBuild(() -> {
            CompletableFuture<GraphRecords.GraphResult> future = CompletableFuture.supplyAsync(() -> {
                        MatchType matchType = request.getMatchType() != null ? request.getMatchType() : MatchType.AUTO;
                        String key = request.getPartitionKey();
                        String leftVal = request.getLeftPartitionValue();
                        String rightVal = request.getRightPartitionValue();
                        boolean isPartitioningApplicable = key != null && !key.isEmpty() &&
                                leftVal != null && !leftVal.isEmpty() &&
                                rightVal != null && !rightVal.isEmpty();

                        CompletableFuture<GraphRecords.GraphResult> result;
                        Instant partitionStart = Instant.now();

                        if (matchType == MatchType.SYMMETRIC || (matchType == MatchType.AUTO && !isPartitioningApplicable)) {
                            log.info("Processing SYMMETRIC match for groupId={}, page={}", groupId, request.getPage());
                            result = symmetricGraphBuilder.build(nodes, request);
                        } else if (matchType == MatchType.BIPARTITE || matchType == MatchType.AUTO) {
                            List<Node> left = new ArrayList<>();
                            List<Node> right = new ArrayList<>();
                            partitionStrategy.partition(nodes.stream(), key, leftVal, rightVal)
                                    .apply((l, r) -> {
                                        left.addAll(l.toList());
                                        right.addAll(r.toList());
                                        return null;
                                    });
                            MatchType inferred = matchType == MatchType.AUTO ? inferMatchType(left, right) : MatchType.BIPARTITE;
                            log.info("Inferred match type: {} for groupId={}, page={}", inferred, groupId, request.getPage());

                            if (inferred == MatchType.SYMMETRIC) {
                                result = symmetricGraphBuilder.build(nodes, request);
                            } else {
                                log.info("Partitioned: groupId={}, page={}, leftNodes={}, rightNodes={}",
                                        groupId, request.getPage(), left.size(), right.size());
                                result = bipartiteGraphBuilder.build(left, right, request);
                            }
                        } else {
                            throw new IllegalArgumentException("Unsupported match type: " + matchType);
                        }

                        meterRegistry.timer("graph_preprocessor_partition", "groupId", groupId)
                                .record(Duration.between(partitionStart, Instant.now()));
                        return result;
                    }, executor)
                    .thenComposeAsync(cf -> cf.orTimeout(BUILD_TIMEOUT_MINUTES, TimeUnit.MINUTES), watchdogExecutor);

            return future.handleAsync((res, throwable) -> {
                sample.stop(meterRegistry.timer("graph_preprocessor_duration", "groupId", groupId, "mode", mode));
                buildSample.stop(meterRegistry.timer("graph_build_duration", "groupId", groupId, "mode", mode));
                if (throwable != null) {
                    Throwable cause = throwable instanceof CompletionException ? throwable.getCause() : throwable;
                    String counterName = cause instanceof TimeoutException ? "graph_build_timeout" : "graph_preprocessor_errors";
                    meterRegistry.counter(counterName, "groupId", groupId, "mode", mode).increment();
                    log.error("Graph build failed for groupId={}, mode={}, page={}:", groupId, mode, request.getPage(), cause);
                    throw cause instanceof RuntimeException ? (RuntimeException) cause : new RuntimeException(cause);
                }
                log.info("Graph build completed for groupId={}, mode={}, page={}", groupId, mode, request.getPage());
                return res;
            }, executor);
        }, groupId, mode, sample, buildSample);
    }

    private CompletableFuture<GraphRecords.GraphResult> acquireAndBuild(
            Supplier<CompletableFuture<GraphRecords.GraphResult>> buildFutureSupplier,
            String groupId, String mode, Timer.Sample sample, Timer.Sample buildSample) {

        boolean acquired = false;

        try {
            acquired = buildSemaphore.tryAcquire(60, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timeout acquiring buildSemaphore for groupId={}", groupId);
                sample.stop(meterRegistry.timer("graph_preprocessor_duration", "groupId", groupId, "mode", mode));
                buildSample.stop(meterRegistry.timer("graph_build_duration", "groupId", groupId, "mode", mode));
                meterRegistry.counter("graph_preprocessor_errors", "groupId", groupId, "mode", mode).increment();
                return CompletableFuture.failedFuture(new RuntimeException("Timeout acquiring semaphore for graph build."));
            }

            log.debug("Semaphore acquired for groupId={}. Remaining permits: {}", groupId, buildSemaphore.availablePermits());

            return buildFutureSupplier.get()
                    .whenComplete((result, throwable) -> {
                        buildSemaphore.release();
                        log.debug("Semaphore released for groupId={}. Remaining permits: {}", groupId, buildSemaphore.availablePermits());
                    });

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Graph build for groupId={} interrupted during semaphore acquisition.", groupId, e);
            sample.stop(meterRegistry.timer("graph_preprocessor_duration", "groupId", groupId, "mode", mode));
            buildSample.stop(meterRegistry.timer("graph_build_duration", "groupId", groupId, "mode", mode));
            meterRegistry.counter("graph_preprocessor_errors", "groupId", groupId, "mode", mode).increment();

            if (acquired) {
                buildSemaphore.release();
            }
            return CompletableFuture.failedFuture(new RuntimeException("Graph build interrupted during semaphore acquisition.", e));
        } catch (Throwable t) {
            log.error("Failed to initiate graph build for groupId={}: {}", groupId, t.getMessage(), t);
            sample.stop(meterRegistry.timer("graph_preprocessor_duration", "groupId", groupId, "mode", mode));
            buildSample.stop(meterRegistry.timer("graph_build_duration", "groupId", groupId, "mode", mode));
            meterRegistry.counter("graph_preprocessor_errors", "groupId", groupId, "mode", mode).increment();
            return CompletableFuture.failedFuture(t);
        }
    }
}