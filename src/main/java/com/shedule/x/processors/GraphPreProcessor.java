package com.shedule.x.processors;

import com.shedule.x.dto.NodeDTO;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.shedule.x.service.GraphRecords.GraphResult;
import org.apache.commons.lang3.tuple.Pair;
import java.util.*;
import java.util.concurrent.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class GraphPreProcessor {
    private static final long HARD_TIMEOUT_MINUTES = 45;

    private final SymmetricGraphBuilderService symmetricGraphBuilder;
    private final BipartiteGraphBuilderService bipartiteGraphBuilder;
    private final EdgePersistence edgePersistence;
    private final PartitionStrategy partitionStrategy;
    private final NodeRepository nodeRepository;
    private final MeterRegistry meterRegistry;
    private final Semaphore buildSemaphore;

    public GraphPreProcessor(
            SymmetricGraphBuilderService symmetricGraphBuilder,
            BipartiteGraphBuilderService bipartiteGraphBuilder,
            @Qualifier("metadataBasedPartitioningStrategy") PartitionStrategy partitionStrategy,
            NodeRepository nodeRepository,
            MeterRegistry meterRegistry,
            @Value("${graph.max-concurrent-builds:2}") int maxConcurrentBuilds,
            EdgePersistence edgePersistence) {

        this.symmetricGraphBuilder = symmetricGraphBuilder;
        this.bipartiteGraphBuilder = bipartiteGraphBuilder;
        this.edgePersistence = edgePersistence;
        this.partitionStrategy = partitionStrategy;
        this.nodeRepository = nodeRepository;
        this.meterRegistry = meterRegistry;

        this.buildSemaphore = new Semaphore(maxConcurrentBuilds, true);
        meterRegistry.gauge("graph_build_queue_length", buildSemaphore, Semaphore::getQueueLength);
    }

    public CompletableFuture<GraphResult> buildGraph(
            List<NodeDTO> nodes,
            MatchingRequest request) {

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


    private CompletableFuture<GraphResult> submitBuildTask(
            List<NodeDTO> nodes,
            MatchingRequest request) {

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
            Pair<Stream<NodeDTO>, Stream<NodeDTO>> partitioned = partitionStrategy.partition(
                    nodes.stream(),
                    request.getPartitionKey(),
                    request.getLeftPartitionValue(),
                    request.getRightPartitionValue()
            );

            List<NodeDTO> left = partitioned.getLeft().toList();
            List<NodeDTO> right = partitioned.getRight().toList();

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


    public MatchType inferMatchType(List<NodeDTO> leftNodes, List<NodeDTO> rightNodes) {
        Set<String> firstTypes = leftNodes.stream().map(NodeDTO::getType).filter(Objects::nonNull)
                .collect(Collectors.toSet());

        Set<String> secondTypes = rightNodes.stream().map(NodeDTO::getType).filter(Objects::nonNull)
                .collect(Collectors.toSet());

        if (firstTypes.isEmpty() || secondTypes.isEmpty()) {
            log.warn("Empty or null node types detected; defaulting to SYMMETRIC");
            return MatchType.SYMMETRIC;
        }

        return firstTypes.equals(secondTypes) ? MatchType.SYMMETRIC : MatchType.BIPARTITE;
    }


    @Transactional(readOnly = true)
    public MatchType determineMatchTypeFromExistingData(UUID groupId, UUID domainId, String cycleId) {
        try (var edgeStream = edgePersistence.streamEdges(domainId, groupId, cycleId)) {
            log.info("→ Opened LMDB edge stream successfully | groupId={} | domainId={}", groupId, domainId);

            AtomicInteger edgeCount = new AtomicInteger(0);
            AtomicReference<MatchType> detected = new AtomicReference<>(MatchType.BIPARTITE);
            AtomicBoolean foundAny = new AtomicBoolean(false);

            edgeStream.forEach(edge -> {
                int currentIndex = edgeCount.incrementAndGet();
                if (currentIndex <= 5) {
                    log.info("→ Edge[{}]: from={} to={} | groupId={}",
                            currentIndex,
                            edge.getFromNodeHash(),
                            edge.getToNodeHash(),
                            groupId
                    );
                }

                if (foundAny.get()) {
                    return;
                }

                String fromHash = edge.getFromNodeHash();
                String toHash = edge.getToNodeHash();

                var fromNodeOpt = nodeRepository.findByReferenceIdAndGroupIdAndDomainId(fromHash, groupId, domainId);
                var toNodeOpt = nodeRepository.findByReferenceIdAndGroupIdAndDomainId(toHash, groupId, domainId);

                if (fromNodeOpt.isEmpty()) {
                    log.info("⚠ fromNodeHash '{}' not found in DB | groupId={} | domainId={}",
                            fromHash, groupId, domainId);
                }
                if (toNodeOpt.isEmpty()) {
                    log.info("⚠ toNodeHash '{}' not found in DB | groupId={} | domainId={}",
                            toHash, groupId, domainId);
                }

                if (fromNodeOpt.isPresent() && toNodeOpt.isPresent()) {

                    String type1 = fromNodeOpt.get().getType();
                    String type2 = toNodeOpt.get().getType();

                    log.info("→ Comparing node types: '{}' vs '{}' | groupId={}", type1, type2, groupId);
                    boolean sameType = Objects.equals(type1, type2);
                    detected.set(sameType ? MatchType.SYMMETRIC : MatchType.BIPARTITE);
                    foundAny.set(true);

                    log.info("✔ First conclusive edge found → Detected MatchType={} | groupId={}",
                            detected.get(), groupId);

                    log.debug("Match type detected from first valid edge: {} | groupId={}", detected.get(), groupId);
                }
            });

            log.info("Total edges scanned from LMDB: {} | groupId={}", edgeCount.get(), groupId);
            MatchType result = foundAny.get() ? detected.get() : MatchType.BIPARTITE;
            log.info("Final match type determined: {} | groupId={}", result, groupId);
            return result;

        } catch (Exception e) {
            log.warn("Failed to determine match type from LMDB edges, falling back to BIPARTITE | groupId={}", groupId, e);
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