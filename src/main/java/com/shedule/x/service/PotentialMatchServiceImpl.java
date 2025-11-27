package com.shedule.x.service;

import com.google.common.collect.Lists;
import com.shedule.x.dto.CursorPage;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodeDTO;
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
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@Slf4j
@Service
public class PotentialMatchServiceImpl implements PotentialMatchService {

    private static final int NODE_FETCH_BATCH_SIZE = 1000;
    private static final int HISTORY_FLUSH_INTERVAL = 5;

    private final NodeFetchService nodeFetchService;
    private final WeightFunctionResolver weightFunctionResolver;
    private final GraphPreProcessor graphPreProcessor;
    private final MeterRegistry meterRegistry;
    private final ExecutorService batchExecutor;
    private final ExecutorService ioExecutor;
    private final MatchParticipationHistoryRepository matchParticipationHistoryRepository;

    private final ConcurrentLinkedQueue<MatchParticipationHistory> historyBuffer = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pageCounter = new AtomicInteger(0);
    // Limit concurrent DB calls for Node Hydration to avoid IO saturation
    private final Semaphore dbFetchSemaphore = new Semaphore(4, true);

    public PotentialMatchServiceImpl(
            NodeFetchService nodeFetchService,
            WeightFunctionResolver weightFunctionResolver,
            GraphPreProcessor graphPreProcessor,
            MeterRegistry meterRegistry,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            @Qualifier("ioExecutorService") ExecutorService ioExecutor,
            MatchParticipationHistoryRepository matchParticipationHistoryRepository
    ) {
        this.nodeFetchService = nodeFetchService;
        this.weightFunctionResolver = weightFunctionResolver;
        this.graphPreProcessor = graphPreProcessor;
        this.meterRegistry = meterRegistry;
        this.batchExecutor = batchExecutor;
        this.ioExecutor = ioExecutor;
        this.matchParticipationHistoryRepository = matchParticipationHistoryRepository;
    }

    @Override
    public CompletableFuture<NodesCount> processNodeBatch(List<UUID> nodeIds, MatchingRequest request) {
        UUID groupId = request.getGroupId();
        UUID domainId = request.getDomainId();
        String cycleId = request.getProcessingCycleId();

        if (nodeIds.isEmpty()) {
            return CompletableFuture.completedFuture(
                    NodesCount.builder().nodeCount(0).build());
        }

        Timer.Sample sample = Timer.start(meterRegistry);

        return fetchNodesInSubBatches(nodeIds, groupId, request.getCreatedAfter())
                .thenComposeAsync(nodes -> {
                    log.info("Hydrated {} nodes for matching | groupId={}", nodes.size(), groupId);

                    bufferMatchParticipationHistory(nodes, groupId, domainId, cycleId);

                    return processGraphAndMatches(nodes, request, groupId, cycleId)
                            .thenApply(v -> NodesCount.builder().nodeCount(nodes.size()).build());

                }, batchExecutor)
                .whenComplete((result, throwable) -> {
                    long duration = sample.stop(meterRegistry.timer("matching_batch_duration", "groupId", groupId.toString()));
                    if (throwable != null) {
                        log.error("Batch failed | groupId={} | cycleId={}", groupId, cycleId, throwable);
                        meterRegistry.counter("matching_batch_errors", "groupId", groupId.toString()).increment();
                    } else {
                        flushHistoryIfNeeded();
                    }
                });
    }

    private CompletableFuture<List<NodeDTO>> fetchNodesInSubBatches(
            List<UUID> nodeIds, UUID groupId, LocalDateTime createdAfter) {

        List<List<UUID>> partitions = Lists.partition(nodeIds, NODE_FETCH_BATCH_SIZE);
        List<CompletableFuture<List<NodeDTO>>> futures = new ArrayList<>();

        for (List<UUID> subBatch : partitions) {
            futures.add(acquireDbSemaphore()
                    .thenCompose(v -> nodeFetchService.fetchNodesInBatchesAsync(subBatch, groupId, createdAfter))
                    .whenComplete((res, ex) -> dbFetchSemaphore.release())
            );
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join) // Safe to join here as they are all done
                        .flatMap(List::stream)
                        .collect(Collectors.toList())
                );
    }

    private CompletableFuture<Void> processGraphAndMatches(
            List<NodeDTO> nodes, MatchingRequest request, UUID groupId, String cycleId) {

        if (nodes.isEmpty()) return CompletableFuture.completedFuture(null);

        request.setNumberOfNodes(nodes.size());
        request.setWeightFunctionKey(weightFunctionResolver.resolveWeightFunctionKey(groupId));

        return graphPreProcessor.buildGraph(nodes, request)
                .thenAcceptAsync(graphResult -> {
                    List<UUID> processedIds = nodes.stream().map(NodeDTO::getId).toList();
                    nodeFetchService.markNodesAsProcessed(processedIds, groupId);
                }, batchExecutor);
    }

    private CompletableFuture<Void> acquireDbSemaphore() {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!dbFetchSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    throw new CompletionException(new TimeoutException("DB Semaphore timeout"));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
        });
    }

    private void bufferMatchParticipationHistory(List<NodeDTO> nodes, UUID groupId, UUID domainId, String cycleId) {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC); // Use consistent clock
        nodes.forEach(node -> historyBuffer.offer(
                MatchParticipationHistory.builder()
                        .participatedAt(now).nodeId(node.getId())
                        .groupId(groupId).domainId(domainId)
                        .processingCycleId(cycleId)
                        .build()));
    }

    private void flushHistoryIfNeeded() {
        if (pageCounter.incrementAndGet() % HISTORY_FLUSH_INTERVAL == 0 && !historyBuffer.isEmpty()) {
            CompletableFuture.runAsync(() -> {
                List<MatchParticipationHistory> entries = new ArrayList<>();
                MatchParticipationHistory entry;
                int count = 0;
                while (count < 2000 && (entry = historyBuffer.poll()) != null) {
                    entries.add(entry);
                    count++;
                }
                if (!entries.isEmpty()) {
                    matchParticipationHistoryRepository.saveAll(entries);
                }
            }, ioExecutor);
        }
    }
}