package com.shedule.x.service;

import com.google.common.collect.Lists;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;
import com.shedule.x.processors.GraphPreProcessor;
import com.shedule.x.processors.WeightFunctionResolver;
import com.shedule.x.models.*;
import com.shedule.x.repo.LastMatchParticipationRepository;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class MatchingServiceImpl implements MatchingService {
    private static final int MIN_LIMIT = 1;
    private static final int MAX_LIMIT = 5000;
    private static final int BATCH_DEFAULT_LIMIT = 500;
    private static final int MAX_NODES = 1000;
    private static final int NODE_ID_SUB_BATCH_SIZE = 500;
    private static final int HISTORY_FLUSH_INTERVAL = 5;

    private final NodeFetchService nodeFetchService;
    private final WeightFunctionResolver weightFunctionResolver;
    private final GraphPreProcessor graphPreProcessor;
    private final MeterRegistry meterRegistry;
    private final ExecutorService batchExecutor;
    private final ExecutorService ioExecutor;
    private final ExecutorService graphExecutor;
    private final LastMatchParticipationRepository lastMatchParticipationRepository;
    private final MatchParticipationHistoryRepository matchParticipationHistoryRepository;
    private final ConcurrentLinkedQueue<MatchParticipationHistory> historyBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pageCounter = new AtomicInteger(0);

    public MatchingServiceImpl(
            NodeFetchService nodeFetchService,
            WeightFunctionResolver weightFunctionResolver,
            GraphPreProcessor graphPreProcessor,
            MeterRegistry meterRegistry,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            @Qualifier("ioExecutorService") ExecutorService ioExecutor,
            @Qualifier("graphExecutorService") ExecutorService graphExecutor,
            LastMatchParticipationRepository lastMatchParticipationRepository,
            MatchParticipationHistoryRepository matchParticipationHistoryRepository
    ) {
        this.nodeFetchService = nodeFetchService;
        this.weightFunctionResolver = weightFunctionResolver;
        this.graphPreProcessor = graphPreProcessor;
        this.meterRegistry = meterRegistry;
        this.batchExecutor = batchExecutor;
        this.ioExecutor = ioExecutor;
        this.graphExecutor = graphExecutor;
        this.lastMatchParticipationRepository = lastMatchParticipationRepository;
        this.matchParticipationHistoryRepository = matchParticipationHistoryRepository;
    }

    @Override
    public CompletableFuture<NodesCount> matchByGroup(MatchingRequest request, int page, String cycleId) {
        int limit = getLimitValue(request, BATCH_DEFAULT_LIMIT);
        Pageable pageable = PageRequest.of(page, limit);
        request.setPage(page);
        return processMatching(request, pageable, "incremental", limit, cycleId);
    }

    private CompletableFuture<NodesCount> processMatching(
            MatchingRequest request, Pageable pageable, String mode, int limit, String cycleId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        String groupId = request.getGroupId();
        UUID domainId = request.getDomainId();
        int page = pageable.getPageNumber();

        return nodeFetchService.fetchNodeIdsAsync(groupId, domainId, pageable, request.getCreatedAfter())
                .thenComposeAsync(nodeIds -> {
                    int numberOfNodes = nodeIds.size();
                    log.info("Fetched {} node IDs for groupId={}, domainId={}, cycleId={}, page={}, createdAfter={}",
                            numberOfNodes, groupId, domainId, cycleId, page, request.getCreatedAfter());
                    if (numberOfNodes > MAX_NODES) {
                        log.warn("Node count {} exceeds maximum {} for groupId={}, domainId={}, cycleId={}, page={}",
                                numberOfNodes, MAX_NODES, groupId, domainId, cycleId, page);
                        meterRegistry.counter("node_count_exceeded", "groupId", groupId, "domainId", domainId.toString(), "cycleId", cycleId).increment();
                        throw new IllegalStateException("Node count exceeds maximum limit");
                    }
                    if (nodeIds.isEmpty()) {
                        log.warn("No node IDs for groupId={}, domainId={}, cycleId={}, page={}", groupId, domainId, cycleId, page);
                        return CompletableFuture.completedFuture(NodesCount.builder().nodeCount(0).hasMoreNodes(false).build());
                    }
                    return fetchNodesInSubBatches(nodeIds, groupId, request.getCreatedAfter())
                            .thenComposeAsync(nodes -> {
                                log.info("{} new nodes fetched for groupId={}, domainId={}, cycleId={}, page={}",
                                        nodes.size(), groupId, domainId, cycleId, page);
                                bufferMatchParticipationHistory(nodes, groupId, domainId, cycleId);
                                return processGraphAndMatchesAsync(nodes, request, groupId, domainId, numberOfNodes, limit, cycleId)
                                        .thenApply(v -> NodesCount.builder()
                                                .nodeCount(numberOfNodes)
                                                .hasMoreNodes(numberOfNodes >= limit)
                                                .build());
                            }, batchExecutor);
                }, batchExecutor)
                .exceptionally(throwable -> {
                    log.error("Matching failed for groupId={}, domainId={}, cycleId={}, page={}: {}",
                            groupId, domainId, cycleId, page, throwable.getMessage());
                    meterRegistry.counter("matching_errors", "groupId", groupId, "domainId", domainId.toString(),
                            "cycleId", cycleId, "mode", mode, "numberOfNodes", "0").increment();
                    return NodesCount.builder().nodeCount(0).hasMoreNodes(false).build();
                })
                .whenComplete((result, throwable) -> {
                    sample.stop(meterRegistry.timer("matching_duration", "groupId", groupId, "domainId", domainId.toString(),
                            "cycleId", cycleId, "mode", mode, "numberOfNodes", String.valueOf(result.nodeCount())));
                    meterRegistry.counter("nodes_processed_total", "groupId", groupId, "domainId", domainId.toString(),
                            "cycleId", cycleId, "mode", mode, "numberOfNodes", String.valueOf(result.nodeCount())).increment(result.nodeCount());
                    flushHistoryIfNeeded();
                });
    }

    private CompletableFuture<List<Node>> fetchNodesInSubBatches(List<UUID> nodeIds, String groupId, LocalDateTime createdAfter) {
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

    private CompletableFuture<Void> processGraphAndMatchesAsync(
            List<Node> nodes, MatchingRequest request, String groupId, UUID domainId, int numberOfNodes, int limit, String cycleId) {
        if (nodes.isEmpty()) {
            log.warn("No nodes fetched for groupId={}, domainId={}, cycleId={}", groupId, domainId, cycleId);
            return CompletableFuture.completedFuture(null);
        }
        request.setNumberOfNodes(nodes.size());
        String weightFunctionKey = weightFunctionResolver.resolveWeightFunctionKey(groupId);
        request.setWeightFunctionKey(weightFunctionKey);

        return graphPreProcessor.buildGraph(nodes, request)
                .thenAcceptAsync(graphResult -> {
                    log.info("Processed graph for groupId={}, domainId={}, cycleId={}, page={}, numberOfNodes={}", groupId, domainId, cycleId, request.getPage(), numberOfNodes);
                    List<UUID> nodeIds = nodes.stream().map(Node::getId).filter(Objects::nonNull).collect(Collectors.toList());
                    nodeFetchService.markNodesAsProcessed(nodeIds, groupId);
                }, graphExecutor)
                .exceptionally(throwable -> {
                    log.error("Graph processing failed for groupId={}, domainId={}, cycleId={}, page={}, numberOfNodes={}: {}",
                            groupId, domainId, cycleId, request.getPage(), numberOfNodes, throwable.getMessage());
                    meterRegistry.counter("graph_processing_errors", "groupId", groupId, "domainId", domainId.toString(),
                            "cycleId", cycleId, "numberOfNodes", String.valueOf(numberOfNodes)).increment();
                    throw new CompletionException("Graph processing failed", throwable);
                });
    }

    private void bufferMatchParticipationHistory(List<Node> nodes, String groupId, UUID domainId, String cycleId) {
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

    private int getLimitValue(MatchingRequest request, int defaultLimit) {
        return request.getLimit() != null
                ? Math.min(Math.max(request.getLimit(), MIN_LIMIT), MAX_LIMIT)
                : defaultLimit;
    }
}