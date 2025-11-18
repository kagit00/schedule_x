package com.shedule.x.processors;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.models.Node;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.partition.PartitionStrategy;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.service.BipartiteGraphBuilderService;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.service.PotentialMatchStreamingService;
import com.shedule.x.service.SymmetricGraphBuilderService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class GraphPreProcessor {
    private static final long SOFT_TIMEOUT_MINUTES = 30;
    private static final long HARD_TIMEOUT_MINUTES = 45;

    private final PotentialMatchStreamingService potentialMatchStreamingService;
    private final SymmetricGraphBuilderService symmetricGraphBuilder;
    private final BipartiteGraphBuilderService bipartiteGraphBuilder;
    private final MeterRegistry meterRegistry;
    private final ExecutorService executor;
    private final ScheduledExecutorService watchdogExecutor;
    private final PartitionStrategy partitionStrategy;
    private final Semaphore buildSemaphore;
    private final NodeRepository nodeRepository;

    public GraphPreProcessor(
            SymmetricGraphBuilderService symmetricGraphBuilder,
            BipartiteGraphBuilderService bipartiteGraphBuilder,
            PotentialMatchStreamingService potentialMatchStreamingService,
            MeterRegistry meterRegistry,
            NodeRepository nodeRepository,
            @Qualifier("graphBuildExecutor") ExecutorService executor,
            @Qualifier("watchdogExecutor") ScheduledExecutorService watchdogExecutor,
            @Qualifier("metadataBasedPartitioningStrategy") PartitionStrategy partitionStrategy,
            @Value("${graph.max-concurrent-builds:2}") int maxConcurrentBuilds) {
        this.symmetricGraphBuilder = Objects.requireNonNull(symmetricGraphBuilder,
                "symmetricGraphBuilder must not be null");
        this.bipartiteGraphBuilder = Objects.requireNonNull(bipartiteGraphBuilder,
                "bipartiteGraphBuilder must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.watchdogExecutor = Objects.requireNonNull(watchdogExecutor, "watchdogExecutor must not be null");
        this.partitionStrategy = Objects.requireNonNull(partitionStrategy, "partitionStrategy must not be null");
        this.nodeRepository = nodeRepository;
        this.potentialMatchStreamingService = potentialMatchStreamingService;
        this.buildSemaphore = new Semaphore(maxConcurrentBuilds, true);
        meterRegistry.gauge("graph_build_queue_length", buildSemaphore, Semaphore::getQueueLength);
    }

    public MatchType inferMatchType(List<Node> leftNodes, List<Node> rightNodes) {
        Set<String> firstTypes = leftNodes.stream().map(Node::getType).filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Set<String> secondTypes = rightNodes.stream().map(Node::getType).filter(Objects::nonNull)
                .collect(Collectors.toSet());
        if (firstTypes.isEmpty() || secondTypes.isEmpty()) {
            log.warn("Empty or null node types detected; defaulting to SYMMETRIC");
            return MatchType.SYMMETRIC;
        }
        return firstTypes.equals(secondTypes) ? MatchType.SYMMETRIC : MatchType.BIPARTITE;
    }

    @Transactional
    public MatchType determineMatchType(UUID groupId, UUID domainId) {
        try (Stream<PotentialMatchEntity> matchStream = potentialMatchStreamingService
                .streamMatches(groupId, domainId, 0, 1)) {

            Optional<PotentialMatchEntity> sampleMatch = matchStream.findFirst();
            if (sampleMatch.isEmpty()) {
                log.warn("No potential matches to determine MatchType for groupId={}, domainId={}", groupId, domainId);
                return MatchType.BIPARTITE;
            }

            PotentialMatchEntity pm = sampleMatch.get();
            String refId = pm.getReferenceId();
            String matchedRefId = pm.getMatchedReferenceId();

            Optional<Node> refNode = nodeRepository.findByReferenceIdAndGroupIdAndDomainId(refId, groupId, domainId);
            Optional<Node> matchedNode = nodeRepository.findByReferenceIdAndGroupIdAndDomainId(matchedRefId, groupId,
                    domainId);

            if (refNode.isEmpty() || matchedNode.isEmpty()) {
                log.warn("Node not found for refId={} or matchedRefId={} in groupId={}, domainId={}",
                        refId, matchedRefId, groupId, domainId);
                return MatchType.BIPARTITE;
            }

            boolean sameType = refNode.get().getType().equals(matchedNode.get().getType());
            log.info("Determined MatchType={} for groupId={}, domainId={} based on node types: {} vs {}",
                    sameType ? MatchType.SYMMETRIC : MatchType.BIPARTITE, groupId, domainId,
                    refNode.get().getType(), matchedNode.get().getType());
            return sameType ? MatchType.SYMMETRIC : MatchType.BIPARTITE;

        } catch (Exception e) {
            log.error("Error determining MatchType for groupId={}, domainId={}: {}", groupId, domainId, e.getMessage());
            return MatchType.BIPARTITE;
        }
    }




    private CompletableFuture<GraphRecords.GraphResult> submitInterruptibleBuildTask(
            List<Node> nodes, MatchingRequest request, UUID groupId, String mode) {

        Thread.currentThread().setName("graph-build-" + groupId + "-" + request.getPage());

        MatchType matchType = request.getMatchType() != null ? request.getMatchType() : MatchType.AUTO;
        String key = request.getPartitionKey();
        String leftVal = request.getLeftPartitionValue();
        String rightVal = request.getRightPartitionValue();
        boolean isPartitioningApplicable = key != null && !key.isEmpty() &&
                leftVal != null && !leftVal.isEmpty() &&
                rightVal != null && !rightVal.isEmpty();

        CompletableFuture<GraphRecords.GraphResult> buildFuture;

        if (matchType == MatchType.SYMMETRIC || (matchType == MatchType.AUTO && !isPartitioningApplicable)) {
            log.info("Processing SYMMETRIC match for groupId={}, page={}", groupId, request.getPage());
            buildFuture = symmetricGraphBuilder.build(nodes, request);
        } else {
            // Partitioning path
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
                log.info("Falling back to SYMMETRIC build after partitioning check");
                buildFuture = symmetricGraphBuilder.build(nodes, request);
            } else {
                log.info("Partitioned: groupId={}, page={}, left={}, right={}",
                        groupId, request.getPage(), left.size(), right.size());
                buildFuture = bipartiteGraphBuilder.build(left, right, request);
            }
        }

        final Future<GraphRecords.GraphResult> taskFuture = new Future<>() {
            private volatile boolean cancelled = false;

            @Override public boolean cancel(boolean mayInterruptIfRunning) {
                cancelled = true;
                buildFuture.cancel(mayInterruptIfRunning);
                return true;
            }
            @Override public boolean isCancelled() { return cancelled || buildFuture.isCancelled(); }
            @Override public boolean isDone() { return buildFuture.isDone(); }
            @Override public GraphRecords.GraphResult get() throws ExecutionException, InterruptedException {
                return buildFuture.get();
            }
            @Override public GraphRecords.GraphResult get(long t, TimeUnit u) throws ExecutionException, InterruptedException, TimeoutException {
                return buildFuture.get(t, u);
            }
        };

        watchdogExecutor.schedule(() -> {
            if (!buildFuture.isDone()) {
                log.warn("SOFT TIMEOUT ({} min) for graph build groupId={}, page={}",
                        SOFT_TIMEOUT_MINUTES, groupId, request.getPage());
                meterRegistry.counter("graph_build_soft_timeout", "groupId", groupId.toString()).increment();
            }
        }, SOFT_TIMEOUT_MINUTES, TimeUnit.MINUTES);

        watchdogExecutor.schedule(() -> {
            if (!buildFuture.isDone()) {
                log.error("HARD TIMEOUT ({} min) → KILLING graph build groupId={}, page={}",
                        HARD_TIMEOUT_MINUTES, groupId, request.getPage());
                meterRegistry.counter("graph_build_hard_timeout", "groupId", groupId.toString()).increment();
                buildFuture.cancel(true);
            }
        }, HARD_TIMEOUT_MINUTES, TimeUnit.MINUTES);

        return buildFuture.applyToEither(
                CompletableFuture.supplyAsync(() -> {
                    try { Thread.sleep(SOFT_TIMEOUT_MINUTES * 60_000); } catch (InterruptedException ignored) {}
                    throw new RuntimeException("Soft timeout");
                }),
                Function.identity()
        );
    }

    public CompletableFuture<GraphRecords.GraphResult> buildGraph(List<Node> nodes, MatchingRequest request) {
        UUID groupId = request.getGroupId();
        String mode = "batch";

        Timer.Sample totalSample = Timer.start(meterRegistry);
        Timer.Sample buildSample = Timer.start(meterRegistry);

        return acquireAndBuildAsync(groupId, mode, () -> {
            return submitInterruptibleBuildTask(nodes, request, groupId, mode);
        })
                .whenComplete((result, throwable) -> {
                    totalSample.stop(meterRegistry.timer("graph_preprocessor_duration",
                            "groupId", groupId.toString(), "mode", mode));
                    buildSample.stop(meterRegistry.timer("graph_build_duration",
                            "groupId", groupId.toString(), "mode", mode));

                    if (throwable != null) {
                        String counterName = (throwable instanceof TimeoutException) ? "graph_build_timeout"
                                : "graph_preprocessor_errors";
                        meterRegistry.counter(counterName, "groupId", groupId.toString(), "mode", mode).increment();
                    }
                });
    }

    private CompletableFuture<GraphRecords.GraphResult> acquireAndBuildAsync(
            UUID groupId,
            String mode,
            Supplier<CompletableFuture<GraphRecords.GraphResult>> buildSupplier) {

        CompletableFuture<Void> semaphoreAcquired = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                if (!buildSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    semaphoreAcquired.completeExceptionally(
                            new RuntimeException("Timeout waiting for graph build slot (semaphore)"));
                } else {
                    log.debug("Semaphore acquired for groupId={}. Permits left: {}",
                            groupId, buildSemaphore.availablePermits());
                    semaphoreAcquired.complete(null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                semaphoreAcquired.completeExceptionally(
                        new RuntimeException("Interrupted during semaphore acquisition", e));
            }
        });

        return semaphoreAcquired.thenCompose(v -> {
            CompletableFuture<GraphRecords.GraphResult> buildFuture = buildSupplier.get();

            return buildFuture.whenComplete((res, ex) -> {
                buildSemaphore.release();
                log.debug("Semaphore released for groupId={}. Permits left: {}",
                        groupId, buildSemaphore.availablePermits());

                if (ex != null) {
                    meterRegistry.counter("graph_preprocessor_errors",
                            "groupId", groupId.toString(), "mode", mode).increment();
                }
            });
        });
    }
}