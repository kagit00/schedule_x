package com.shedule.x.service;

import com.google.common.collect.Lists;
import com.shedule.x.dto.CursorPage;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;
import com.shedule.x.processors.GraphPreProcessor;
import com.shedule.x.processors.WeightFunctionResolver;
import com.shedule.x.models.*;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
@Service
public class PotentialMatchServiceImpl implements PotentialMatchService {
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
    public CompletableFuture<NodesCount> matchByGroup(MatchingRequest request, String cycleId) {
        int limit = request.getLimit();
        UUID groupId = request.getGroupId();
        UUID domainId = request.getDomainId();

        Timer.Sample sample = Timer.start(meterRegistry);

        return fetchNodeIdsByCursor(groupId, domainId, limit, cycleId)
                .thenComposeAsync(page -> {
                    List<UUID> nodeIds = page.ids();
                    int fetchedCount = nodeIds.size();

                    log.info("Fetched {} node IDs for groupId={}, domainId={}, cycleId={}",
                            fetchedCount, groupId, domainId, cycleId);

                    if (fetchedCount == 0) {
                        return CompletableFuture.completedFuture(
                                NodesCount.builder().nodeCount(0).hasMoreNodes(false).build());
                    }

                    if (fetchedCount > MAX_NODES) {
                        log.warn("Node count {} exceeds MAX_NODES={} for groupId={}, cycleId={}",
                                fetchedCount, MAX_NODES, groupId, cycleId);
                        meterRegistry.counter("node_count_exceeded",
                                Tags.of("groupId", groupId.toString(), "cycleId", cycleId)).increment();
                        return CompletableFuture.failedFuture(
                                new IllegalStateException("Node count exceeds maximum limit"));
                    }

                    return fetchNodesInSubBatches(nodeIds, groupId, request.getCreatedAfter())
                            .thenComposeAsync(nodes -> {
                                log.info("Loaded {} nodes for matching, groupId={}, cycleId={}",
                                        nodes.size(), groupId, cycleId);

                                bufferMatchParticipationHistory(nodes, groupId, domainId, cycleId);

                                return processGraphAndMatches(nodes, request, groupId, domainId, cycleId)
                                        .thenApply(v -> NodesCount.builder()
                                                .nodeCount(fetchedCount)
                                                .hasMoreNodes(page.hasMore())
                                                .build());
                            }, batchExecutor);
                }, batchExecutor)
                .exceptionally(throwable -> {
                    log.error("Matching failed for groupId={}, domainId={}, cycleId={}: {}",
                            groupId, domainId, cycleId, throwable.getMessage(), throwable);
                    meterRegistry.counter("matching_errors",
                                    Tags.of("groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId))
                            .increment();
                    return NodesCount.builder().nodeCount(0).hasMoreNodes(false).build();
                })
                .whenComplete((result, throwable) -> {
                    sample.stop(meterRegistry.timer("matching_duration",
                            Tags.of("groupId", groupId.toString(),
                                    "domainId", domainId.toString(),
                                    "cycleId", cycleId,
                                    "nodeCount", String.valueOf(result.nodeCount()))));

                    meterRegistry.counter("nodes_processed_total",
                                    Tags.of("groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId))
                            .increment(result.nodeCount());

                    flushHistoryIfNeeded();
                });
    }

    private CompletableFuture<CursorPage> fetchNodeIdsByCursor(
            UUID groupId, UUID domainId, int limit, String cycleId) {
        return nodeFetchService.fetchNodeIdsByCursor(groupId, domainId, limit, cycleId);
    }

    private CompletableFuture<List<Node>> fetchNodesInSubBatches(
            List<UUID> nodeIds, UUID groupId, LocalDateTime createdAfter) {

        List<List<UUID>> subBatches = Lists.partition(nodeIds, NODE_ID_SUB_BATCH_SIZE);

        if (subBatches.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        List<Node> allNodes = Collections.synchronizedList(new ArrayList<>());
        Semaphore semaphore = new Semaphore(4);

        List<CompletableFuture<Void>> futures = subBatches.stream()
                .map(subBatch -> acquireSemaphoreAsync(semaphore)
                        .thenComposeAsync(v ->
                                        nodeFetchService.fetchNodesInBatchesAsync(subBatch, groupId, createdAfter)
                                                .thenAccept(allNodes::addAll)
                                                .whenComplete((res, ex) -> semaphore.release()),
                                batchExecutor))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> new ArrayList<>(allNodes));
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
        });
        return future;
    }

    private CompletableFuture<Void> processGraphAndMatches(
            List<Node> nodes,
            MatchingRequest request,
            UUID groupId,
            UUID domainId,
            String cycleId) {

        if (nodes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        request.setNumberOfNodes(nodes.size());
        String weightFunctionKey = weightFunctionResolver.resolveWeightFunctionKey(groupId);
        request.setWeightFunctionKey(weightFunctionKey);

        return graphPreProcessor.buildGraph(nodes, request)
                .thenAcceptAsync(graphResult -> {
                    log.info("Graph built for {} nodes, groupId={}, cycleId={}", nodes.size(), groupId, cycleId);

                    List<UUID> nodeIdsToMark = nodes.stream()
                            .map(Node::getId)
                            .filter(Objects::nonNull)
                            .toList();

                    if (!nodeIdsToMark.isEmpty()) {
                        nodeFetchService.markNodesAsProcessed(nodeIdsToMark, groupId);
                        log.debug("Marked {} nodes as processed, groupId={}", nodeIdsToMark.size(), groupId);
                    }
                }, graphExecutor)
                .exceptionally(throwable -> {
                    log.error("Graph processing failed for groupId={}, cycleId={}: {}",
                            groupId, cycleId, throwable.getMessage(), throwable);
                    meterRegistry.counter("graph_processing_errors",
                            Tags.of("groupId", groupId.toString(), "cycleId", cycleId)).increment();
                    throw new CompletionException("Graph processing failed", throwable);
                });
    }

    private void bufferMatchParticipationHistory(List<Node> nodes, UUID groupId, UUID domainId, String cycleId) {
        LocalDateTime now = DefaultValuesPopulator.getCurrentTimestamp();
        List<MatchParticipationHistory> entries = nodes.stream()
                .map(node -> MatchParticipationHistory.builder()
                        .participatedAt(now)
                        .nodeId(node.getId())
                        .groupId(groupId)
                        .domainId(domainId)
                        .build())
                .toList();
        historyBuffer.addAll(entries);
    }

    private void flushHistoryIfNeeded() {
        if (pageCounter.incrementAndGet() % HISTORY_FLUSH_INTERVAL == 0 && !historyBuffer.isEmpty()) {
            CompletableFuture.runAsync(() -> {
                List<MatchParticipationHistory> entries = new ArrayList<>();
                MatchParticipationHistory entry;
                while ((entry = historyBuffer.poll()) != null) {
                    entries.add(entry);
                }
                try {
                    matchParticipationHistoryRepository.saveAll(entries);
                    log.info("Flushed {} match participation history entries", entries.size());
                } catch (Exception e) {
                    log.warn("Failed to flush match participation history: {}", e.getMessage());
                }
            }, ioExecutor);
        }
    }
}