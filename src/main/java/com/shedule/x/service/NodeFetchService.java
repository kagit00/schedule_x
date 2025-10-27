package com.shedule.x.service;

import java.time.LocalDateTime;
import java.util.*;
import com.shedule.x.models.Node;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.utils.db.BatchUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class NodeFetchService {
    private final NodeRepository nodeRepository;
    private final MeterRegistry meterRegistry;
    private final Executor executor;
    private final TransactionTemplate transactionTemplate;
    private final long futureTimeoutSeconds;

    @Value("${node-fetch.batch-size:1000}")
    private int batchSize;

    public NodeFetchService(
            NodeRepository nodeRepository,
            MeterRegistry meterRegistry,
            @Qualifier("nodesFetchExecutor") Executor executor,
            @Value("${node-fetch.future-timeout-seconds:30}") long futureTimeoutSeconds,
            PlatformTransactionManager transactionManager
    ) {
        this.nodeRepository = nodeRepository;
        this.meterRegistry = meterRegistry;
        this.executor = executor;
        this.futureTimeoutSeconds = futureTimeoutSeconds;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setReadOnly(true);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public CompletableFuture<List<UUID>> fetchNodeIdsAsync(UUID groupId, UUID domainId, Pageable pageable, LocalDateTime createdAfter) {
        Timer.Sample sample = Timer.start(meterRegistry);
        if (groupId == null || domainId == null || pageable == null) {
            log.error("Invalid input: groupId={}, domainId={}, pageable={}", groupId, domainId, pageable);
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return CompletableFuture.supplyAsync(() ->
                        transactionTemplate.execute(status -> {
                            try {
                                List<UUID> result = nodeRepository.findIdsByGroupIdAndDomainId(groupId, domainId, pageable, createdAfter);
                                log.info("Fetched {} node IDs for groupId={}", result.size(), groupId);
                                meterRegistry.counter("node_ids_fetched_total", Tags.of("groupId", groupId.toString())).increment(result.size());
                                return result;
                            } catch (Exception e) {
                                meterRegistry.counter("node_fetch_ids_errors", Tags.of("groupId", groupId.toString())).increment();
                                log.error("Failed to fetch node IDs for groupId={}", groupId, e);
                                throw e;
                            }
                        }), executor)
                .orTimeout(futureTimeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(throwable -> {
                    log.error("Async fetchNodeIds failed for groupId={}", groupId, throwable);
                    meterRegistry.counter("node_fetch_ids_errors", Tags.of("groupId", groupId.toString())).increment();
                    return Collections.emptyList();
                })
                .whenComplete((result, throwable) ->
                        sample.stop(meterRegistry.timer("node_fetch_ids_duration", Tags.of("groupId", groupId.toString()))));
    }

    public CompletableFuture<List<Node>> fetchNodesInBatchesAsync(List<UUID> nodeIds, UUID groupId, LocalDateTime createdAfter) {
        Timer.Sample sample = Timer.start(meterRegistry);
        if (nodeIds == null || nodeIds.isEmpty() || groupId == null) {
            log.warn("Invalid input: nodeIds={} or groupId={} is null/empty", nodeIds, groupId);
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        List<List<UUID>> batches = BatchUtils.partition(nodeIds, batchSize);
        List<CompletableFuture<List<Node>>> futures = batches.stream()
                .map(batch -> nodeRepository.findByIdsWithMetadataAsync(batch)
                        .orTimeout(futureTimeoutSeconds, TimeUnit.SECONDS)
                        .exceptionally(throwable -> {
                            log.error("Failed to fetch batch for groupId={}, batchSize={}", groupId, batch.size(), throwable);
                            meterRegistry.counter("node_fetch_batch_errors", Tags.of("groupId", groupId.toString())).increment();
                            return Collections.emptyList();
                        }))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(futureTimeoutSeconds * 2, TimeUnit.SECONDS)
                .thenApply(v -> {
                    List<Node> all = futures.stream()
                            .map(CompletableFuture::join)
                            .flatMap(List::stream)
                            .filter(n -> createdAfter == null || n.getCreatedAt() == null || !n.getCreatedAt().isBefore(createdAfter))
                            .toList();
                    log.info("Fetched {} nodes for groupId={}", all.size(), groupId);
                    meterRegistry.counter("nodes_fetched_total", Tags.of("groupId", groupId.toString())).increment(all.size());
                    return all;
                })
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to fetch nodes for groupId={}", groupId, throwable);
                        meterRegistry.counter("node_fetch_nodes_errors", Tags.of("groupId", groupId.toString())).increment();
                    }
                    sample.stop(meterRegistry.timer("node_fetch_by_ids_duration", Tags.of("groupId", groupId.toString())));
                });
    }


    @Transactional
    public void markNodesAsProcessed(List<UUID> nodeIds, UUID groupId) {
        if (nodeIds == null || nodeIds.isEmpty() || groupId == null) {
            return;
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            nodeRepository.markAsProcessed(nodeIds);
            log.info("Marked {} nodes as processed for groupId={}", nodeIds.size(), groupId);
            meterRegistry.counter("node_mark_processed_total", Tags.of("groupId", groupId.toString())).increment(nodeIds.size());
        } catch (Exception e) {
            log.error("Failed to mark nodes as processed for groupId={}", nodeIds.size(), e);
            meterRegistry.counter("node_mark_processed_errors", Tags.of("groupId", groupId.toString())).increment();
            throw e;
        } finally {
            sample.stop(meterRegistry.timer("node_mark_processed_duration", Tags.of("groupId", groupId.toString())));
        }
    }
}