package com.shedule.x.processors;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.models.Node;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.partition.PartitionStrategy;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.service.BipartiteGraphBuilderService;
import com.shedule.x.service.PotentialMatchStreamingService;
import com.shedule.x.service.SymmetricGraphBuilderService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;


import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.shedule.x.service.GraphRecords.GraphResult;
import org.apache.commons.lang3.tuple.Pair;
import java.util.*;
import java.util.concurrent.*;


@Slf4j
@Component
public class GraphPreProcessor {
    private static final long HARD_TIMEOUT_MINUTES = 45;

    private final SymmetricGraphBuilderService symmetricGraphBuilder;
    private final BipartiteGraphBuilderService bipartiteGraphBuilder;
    private final PotentialMatchStreamingService potentialMatchStreamingService;
    private final PartitionStrategy partitionStrategy;
    private final NodeRepository nodeRepository;
    private final MeterRegistry meterRegistry;
    private final Semaphore buildSemaphore;

    public GraphPreProcessor(
            SymmetricGraphBuilderService symmetricGraphBuilder,
            BipartiteGraphBuilderService bipartiteGraphBuilder,
            PotentialMatchStreamingService potentialMatchStreamingService,
            @Qualifier("metadataBasedPartitioningStrategy") PartitionStrategy partitionStrategy,
            NodeRepository nodeRepository,
            MeterRegistry meterRegistry,
            @Value("${graph.max-concurrent-builds:2}") int maxConcurrentBuilds) {

        this.symmetricGraphBuilder = symmetricGraphBuilder;
        this.bipartiteGraphBuilder = bipartiteGraphBuilder;
        this.potentialMatchStreamingService = potentialMatchStreamingService;
        this.partitionStrategy = partitionStrategy;
        this.nodeRepository = nodeRepository;
        this.meterRegistry = meterRegistry;

        this.buildSemaphore = new Semaphore(maxConcurrentBuilds, true);
        meterRegistry.gauge("graph_build_queue_length", buildSemaphore, Semaphore::getQueueLength);
    }

    public CompletableFuture<GraphResult> buildGraph(List<Node> nodes, MatchingRequest request) {
        UUID groupId = request.getGroupId();
        Timer.Sample totalSample = Timer.start(meterRegistry);

        return acquireAndBuildAsync(groupId, () -> submitBuildTask(nodes, request))
                .whenComplete((result, throwable) -> {
                    totalSample.stop(meterRegistry.timer("graph_preprocessor_duration", "groupId", groupId.toString()));

                    if (throwable != null) {
                        log.error("Graph Build Failed | groupId={}", groupId, throwable);
                        String errorType = (throwable instanceof TimeoutException) ? "timeout" : "error";
                        meterRegistry.counter("graph_build_failure", "type", errorType, "groupId", groupId.toString()).increment();
                    }
                });
    }


    private CompletableFuture<GraphResult> submitBuildTask(List<Node> nodes, MatchingRequest request) {
        UUID groupId = request.getGroupId();
        CompletableFuture<GraphResult> buildFuture;

        String key = request.getPartitionKey();
        boolean hasPartitionConfig = key != null && !key.isEmpty()
                && request.getLeftPartitionValue() != null
                && request.getRightPartitionValue() != null;

        if (!hasPartitionConfig) {
            log.info("Missing partition config -> Routing to SYMMETRIC build | groupId={}", groupId);
            buildFuture = symmetricGraphBuilder.build(nodes, request);

        } else {
            Pair<Stream<Node>, Stream<Node>> partitioned = partitionStrategy.partition(
                    nodes.stream(),
                    request.getPartitionKey(),
                    request.getLeftPartitionValue(),
                    request.getRightPartitionValue()
            );

            List<Node> left = partitioned.getLeft().toList();
            List<Node> right = partitioned.getRight().toList();

            MatchType matchType = request.getMatchType() == MatchType.AUTO
                    ? inferMatchType(left, right)
                    : request.getMatchType();

            if (matchType == MatchType.SYMMETRIC) {
                log.info("Inferred SYMMETRIC -> Routing to SYMMETRIC build | groupId={}", groupId);
                buildFuture = symmetricGraphBuilder.build(nodes, request);
            } else {
                log.info("Routing to BIPARTITE build | groupId={} | Strategy={}", groupId, partitionStrategy.getClass().getSimpleName());
                log.info("Partitioned Nodes: Left={} | Right={} | groupId={}", left.size(), right.size(), groupId);
                buildFuture = bipartiteGraphBuilder.build(left, right, request);
            }
        }

        // 3. Attach Timeout (Native Async)
        return buildFuture
                .orTimeout(HARD_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                .exceptionally(ex -> {
                    if (ex instanceof TimeoutException) {
                        log.error("HARD TIMEOUT ({} min) | groupId={}", HARD_TIMEOUT_MINUTES, groupId);
                        buildFuture.cancel(true);
                    }
                    throw new CompletionException(ex);
                });
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

    @Transactional(readOnly = true)
    public MatchType determineMatchTypeFromExistingData(UUID groupId, UUID domainId) {
        try (Stream<PotentialMatchEntity> matchStream = potentialMatchStreamingService
                .streamMatches(groupId, domainId, 0, 1)) {

            return matchStream.findFirst()
                    .flatMap(pm -> {
                        var ref = nodeRepository.findByReferenceIdAndGroupIdAndDomainId(pm.getReferenceId(), groupId, domainId);
                        var mat = nodeRepository.findByReferenceIdAndGroupIdAndDomainId(pm.getMatchedReferenceId(), groupId, domainId);
                        if (ref.isPresent() && mat.isPresent()) {
                            boolean sameType = Objects.equals(ref.get().getType(), mat.get().getType());
                            return Optional.of(sameType ? MatchType.SYMMETRIC : MatchType.BIPARTITE);
                        }
                        return Optional.empty();
                    })
                    .orElse(MatchType.BIPARTITE);
        } catch (Exception e) {
            log.warn("Could not determine match type from DB, defaulting to Bipartite", e);
            return MatchType.BIPARTITE;
        }
    }

    private CompletableFuture<GraphResult> acquireAndBuildAsync(
            UUID groupId,
            Supplier<CompletableFuture<GraphResult>> buildSupplier) {

        return CompletableFuture.runAsync(() -> {
            try {
                if (!buildSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    throw new CompletionException(new TimeoutException("Semaphore wait timed out"));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
        }).thenCompose(v -> {
            log.debug("Build Semaphore Acquired | groupId={}", groupId);
            return buildSupplier.get().whenComplete((r, e) -> {
                buildSemaphore.release();
                log.debug("Build Semaphore Released | groupId={}", groupId);
            });
        });
    }
}