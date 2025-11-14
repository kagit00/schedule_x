package com.shedule.x.service;

import com.google.common.collect.Lists;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;
import com.shedule.x.processors.GraphPreProcessor;
import com.shedule.x.processors.WeightFunctionResolver;
import com.shedule.x.models.*;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class PotentialMatchServiceImpl implements PotentialMatchService {
    private static final int MIN_LIMIT = 1;
    private static final int MAX_LIMIT = 5000;
    private static final int BATCH_DEFAULT_LIMIT = 1000;
    private static final int MAX_NODES = 2000;
    private static final int NODE_ID_SUB_BATCH_SIZE = 1000;
    private static final int HISTORY_FLUSH_INTERVAL = 5;

    private final NodeFetchService nodeFetchService;
    private final WeightFunctionResolver weightFunctionResolver;
    private final GraphPreProcessor graphPreProcessor;
    private final MeterRegistry meterRegistry;
    private final ExecutorService batchExecutor;
    private final ExecutorService ioExecutor;
    private final ExecutorService graphExecutor;
    private final MatchParticipationHistoryRepository matchParticipationHistoryRepository;
    private final ConcurrentLinkedQueue<MatchParticipationHistory> historyBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pageCounter = new AtomicInteger(0);
    @Value("${match.batch-overlap:200}") private int batchOverlap;

    public PotentialMatchServiceImpl(
            NodeFetchService nodeFetchService,
            WeightFunctionResolver weightFunctionResolver,
            GraphPreProcessor graphPreProcessor,
            MeterRegistry meterRegistry,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            @Qualifier("ioExecutorService") ExecutorService ioExecutor,
            @Qualifier("graphExecutorService") ExecutorService graphExecutor,
            MatchParticipationHistoryRepository matchParticipationHistoryRepository
    ) {
        this.nodeFetchService = nodeFetchService;
        this.weightFunctionResolver = weightFunctionResolver;
        this.graphPreProcessor = graphPreProcessor;
        this.meterRegistry = meterRegistry;
        this.batchExecutor = batchExecutor;
        this.ioExecutor = ioExecutor;
        this.graphExecutor = graphExecutor;
        this.matchParticipationHistoryRepository = matchParticipationHistoryRepository;
    }

    @Override
    public CompletableFuture<NodesCount> matchByGroup(MatchingRequest request, int startOffset, int limit, String cycleId) {
        request.setLimit(limit);
        return processMatching(request, startOffset, limit, cycleId);
    }

    private CompletableFuture<NodesCount> processMatching(
            MatchingRequest request, int startOffset, int limit, String cycleId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        UUID groupId = request.getGroupId();
        UUID domainId = request.getDomainId();

        return nodeFetchService.fetchNodeIdsByOffsetAsync(groupId, domainId, startOffset, limit, request.getCreatedAfter())
                .thenComposeAsync(nodeIds -> {
                    int numberOfNodes = nodeIds.size();
                    log.info("Fetched {} node IDs for groupId={}, domainId={}, cycleId={}, startOffset={}, limit={}, createdAfter={}",
                            numberOfNodes, groupId, domainId, cycleId, startOffset, limit, request.getCreatedAfter());

                    if (numberOfNodes > MAX_NODES) {
                        log.warn("Node count {} exceeds maximum {} for groupId={}, domainId={}, cycleId={}, startOffset={}",
                                numberOfNodes, MAX_NODES, groupId, domainId, cycleId, startOffset);
                        meterRegistry.counter("node_count_exceeded", "groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId).increment();
                        throw new IllegalStateException("Node count exceeds maximum limit");
                    }

                    if (nodeIds.isEmpty()) {
                        log.warn("No node IDs for groupId={}, domainId={}, cycleId={}, startOffset={}", groupId, domainId, cycleId, startOffset);
                        return CompletableFuture.completedFuture(NodesCount.builder().nodeCount(0).hasMoreNodes(false).build());
                    }

                    return fetchNodesInSubBatches(nodeIds, groupId, request.getCreatedAfter())
                            .thenComposeAsync(nodes -> {
                                log.info("{} new nodes fetched for groupId={}, domainId={}, cycleId={}, startOffset={}",
                                        nodes.size(), groupId, domainId, cycleId, startOffset);
                                bufferMatchParticipationHistory(nodes, groupId, domainId, cycleId);
                                return processGraphAndMatches(nodes, request, groupId, domainId, numberOfNodes, limit, cycleId, startOffset)
                                        .thenApply(v -> NodesCount.builder()
                                                .nodeCount(numberOfNodes)
                                                .hasMoreNodes(numberOfNodes >= limit)
                                                .build());
                            }, batchExecutor);
                }, batchExecutor)
                .exceptionally(throwable -> {
                    log.error("Matching failed for groupId={}, domainId={}, cycleId={}, startOffset={}: {}",
                            groupId, domainId, cycleId, startOffset, throwable.getMessage());
                    meterRegistry.counter("matching_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                            "cycleId", cycleId, "numberOfNodes", "0").increment();
                    return NodesCount.builder().nodeCount(0).hasMoreNodes(false).build();
                })
                .whenComplete((result, throwable) -> {
                    sample.stop(meterRegistry.timer("matching_duration", "groupId", groupId.toString(), "domainId", domainId.toString(),
                            "cycleId", cycleId, "numberOfNodes", String.valueOf(result.nodeCount())));
                    meterRegistry.counter("nodes_processed_total", "groupId", groupId.toString(), "domainId", domainId.toString(),
                            "cycleId", cycleId, "numberOfNodes", String.valueOf(result.nodeCount())).increment(result.nodeCount());
                    flushHistoryIfNeeded();
                });
    }


    private CompletableFuture<List<Node>> fetchNodesInSubBatches(List<UUID> nodeIds, UUID groupId, LocalDateTime createdAfter) {
        List<List<UUID>> subBatches = Lists.partition(nodeIds, NODE_ID_SUB_BATCH_SIZE);
        Semaphore semaphore = new Semaphore(4);
        List<CompletableFuture<List<Node>>> futures = new ArrayList<>();

        for (List<UUID> subBatch : subBatches) {
            try {
                boolean acquired = semaphore.tryAcquire(60, TimeUnit.SECONDS);
                if (!acquired) {
                    log.warn("Timed out acquiring semaphore for groupId={}, subBatch size={}", groupId, subBatch.size());
                    continue;
                }

                futures.add(nodeFetchService.fetchNodesInBatchesAsync(subBatch, groupId, createdAfter)
                        .whenComplete((v, t) -> semaphore.release()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.failedFuture(e);
            }
        }

        CompletableFuture<List<Node>> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenAcceptAsync(v -> {
                    try {
                        List<Node> nodes = futures.stream()
                                .flatMap(f -> {
                                    try {
                                        return f.get(300, TimeUnit.SECONDS).stream();
                                    } catch (Exception e) {
                                        log.error("Failed to get future result for groupId={}: {}", groupId, e.getMessage());
                                        return Stream.empty();
                                    }
                                })
                                .collect(Collectors.toList());
                        resultFuture.complete(nodes);
                    } catch (Exception e) {
                        resultFuture.completeExceptionally(e);
                    }
                }, batchExecutor)
                .exceptionally(t -> {
                    log.error("Failed to combine futures for groupId={}: {}", groupId, t.getMessage());
                    resultFuture.completeExceptionally(t);
                    return null;
                });

        return resultFuture;
    }

    private CompletableFuture<Void> processGraphAndMatches(
            List<Node> nodes, MatchingRequest request, UUID groupId, UUID domainId,
            int numberOfNodes, int limit, String cycleId, int startOffset) {
        if (nodes.isEmpty()) {
            log.warn("No nodes fetched for groupId={}, domainId={}, cycleId={}, startOffset={}", groupId, domainId, cycleId, startOffset);
            return CompletableFuture.completedFuture(null);
        }

        request.setNumberOfNodes(nodes.size());
        String weightFunctionKey = weightFunctionResolver.resolveWeightFunctionKey(groupId);
        request.setWeightFunctionKey(weightFunctionKey);

        return graphPreProcessor.buildGraph(nodes, request)
                .thenAcceptAsync(graphResult -> {
                    log.info("Processed graph for groupId={}, domainId={}, cycleId={}, startOffset={}, numberOfNodes={}",
                            groupId, domainId, cycleId, startOffset, numberOfNodes);

                    // Only mark non-overlap prefix as processed
                    int cutoff = Math.max(0, nodes.size() - batchOverlap);
                    List<UUID> nodeIdsToMark = nodes.stream()
                            .map(Node::getId)
                            .filter(Objects::nonNull)
                            .limit(cutoff)
                            .collect(Collectors.toList());

                    if (!nodeIdsToMark.isEmpty()) {
                        nodeFetchService.markNodesAsProcessed(nodeIdsToMark, groupId);
                        log.debug("Marked {} nodes as processed (overlap={}, total={}) for groupId={}, startOffset={}",
                                nodeIdsToMark.size(), batchOverlap, nodes.size(), groupId, startOffset);
                    } else {
                        log.debug("No nodes to mark for startOffset={} (nodes={} overlap={})",
                                startOffset, nodes.size(), batchOverlap);
                    }
                }, graphExecutor)
                .exceptionally(throwable -> {
                    log.error("Graph processing failed for groupId={}, domainId={}, cycleId={}, startOffset={}, numberOfNodes={}: {}",
                            groupId, domainId, cycleId, startOffset, numberOfNodes, throwable.getMessage());
                    meterRegistry.counter("graph_processing_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                            "cycleId", cycleId, "numberOfNodes", String.valueOf(numberOfNodes)).increment();
                    throw new CompletionException("Graph processing failed", throwable);
                });
    }


    private void bufferMatchParticipationHistory(List<Node> nodes, UUID groupId, UUID domainId, String cycleId) {
        LocalDateTime now = DefaultValuesPopulator.getCurrentTimestamp();
        List<MatchParticipationHistory> historyEntries = nodes.stream()
                .map(node -> MatchParticipationHistory.builder()
                        .participatedAt(now)
                        .nodeId(node.getId())
                        .groupId(groupId)
                        .domainId(domainId)
                        .build())
                .toList();
        historyBuffer.addAll(historyEntries);
    }


    private void flushHistoryIfNeeded() {
        if (pageCounter.incrementAndGet() % HISTORY_FLUSH_INTERVAL == 0 && !historyBuffer.isEmpty()) {
            CompletableFuture.runAsync(() -> {
                try {
                    List<MatchParticipationHistory> entries = new ArrayList<>();
                    while (!historyBuffer.isEmpty()) {
                        entries.add(historyBuffer.poll());
                    }
                    matchParticipationHistoryRepository.saveAll(entries);
                    log.info("Flushed {} match participation history entries", entries.size());
                } catch (Exception e) {
                    log.warn("Failed to flush match participation history: {}", e.getMessage());
                }
            }, ioExecutor);
        }
    }
}