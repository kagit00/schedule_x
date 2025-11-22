package com.shedule.x.service;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;

import com.shedule.x.dto.CursorPage;
import com.shedule.x.repo.NodeCursorProjection;
import com.shedule.x.models.Node;
import com.shedule.x.models.NodesCursor;
import com.shedule.x.models.NodesCursorId;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.repo.NodesCursorRepository;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class NodeFetchService {
    private final NodeRepository nodeRepository;
    private final MeterRegistry meterRegistry;
    private final Executor executor;
    private final NodesCursorRepository nodesCursorRepository;
    private final long futureTimeoutSeconds = 30;

    public NodeFetchService(
            NodeRepository nodeRepository,
            MeterRegistry meterRegistry,
            @Qualifier("nodesFetchExecutor") Executor executor,
            NodesCursorRepository nodesCursorRepository
    ) {
        this.nodeRepository = nodeRepository;
        this.meterRegistry = meterRegistry;
        this.executor = executor;
        this.nodesCursorRepository = nodesCursorRepository;
    }

    public CompletableFuture<List<Node>> fetchNodesInBatchesAsync(List<UUID> nodeIds, UUID groupId, LocalDateTime createdAfter) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        Timer.Sample sample = Timer.start(meterRegistry);

        return CompletableFuture.supplyAsync(() -> {
                    try {
                        List<Node> nodes = nodeRepository.findByIdsWithMetadata(nodeIds);

                        // Filter in memory (cheaper than complex DB predicates if batch is small)
                        if (createdAfter != null) {
                            return nodes.stream()
                                    .filter(n -> n.getCreatedAt() == null || !n.getCreatedAt().isBefore(createdAfter))
                                    .toList();
                        }
                        return nodes;
                    } catch (Exception e) {
                        log.error("Failed to hydrate nodes for groupId={}", groupId, e);
                        meterRegistry.counter("node_fetch_error", "groupId", groupId.toString()).increment();
                        throw e;
                    }
                }, executor)
                .orTimeout(futureTimeoutSeconds, TimeUnit.SECONDS)
                .whenComplete((res, ex) ->
                        sample.stop(meterRegistry.timer("node_fetch_hydration_duration", "groupId", groupId.toString()))
                );
    }


    @Transactional
    public void markNodesAsProcessed(List<UUID> nodeIds, UUID groupId) {
        if (nodeIds == null || nodeIds.isEmpty()) return;

        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            nodeRepository.markAsProcessed(nodeIds);
        } catch (Exception e) {
            log.error("Failed to mark nodes processed | groupId={}", groupId, e);
            meterRegistry.counter("node_mark_error", "groupId", groupId.toString()).increment();
            throw e;
        } finally {
            sample.stop(meterRegistry.timer("node_mark_processed_duration", "groupId", groupId.toString()));
        }
    }

    public CompletableFuture<CursorPage> fetchNodeIdsByCursor(
            UUID groupId, UUID domainId, int limit, String cycleId) {

        Timer.Sample sample = Timer.start(meterRegistry);

        return CompletableFuture.supplyAsync(() -> {
                    // 1. Get Current Cursor State
                    NodesCursor cursor = nodesCursorRepository
                            .findByIdGroupIdAndIdDomainId(groupId, domainId)
                            .orElse(null);

                    LocalDateTime cursorTime = (cursor != null && cursor.getCursorCreatedAt() != null)
                            ? cursor.getCursorCreatedAt().toLocalDateTime() : null;
                    UUID cursorId = (cursor != null) ? cursor.getCursorId() : null;

                    // 2. Fetch Page (ID + CreatedAt) in ONE query
                    // Return type: List<NodeCursorProjection> or List<Tuple>
                    List<NodeCursorProjection> page = nodeRepository.findUnprocessedNodeIdsAndDatesByCursor(
                            groupId, domainId, cursorTime, cursorId, limit);

                    if (page.isEmpty()) {
                        return new CursorPage(Collections.emptyList(), false, null, null);
                    }

                    // 3. Extract IDs and New Cursor Data
                    List<UUID> ids = new ArrayList<>(page.size());
                    for (NodeCursorProjection p : page) {
                        ids.add(p.getId());
                    }

                    NodeCursorProjection lastItem = page.get(page.size() - 1);
                    return new CursorPage(ids, true, lastItem.getCreatedAt(), lastItem.getId());

                }, executor)
                .orTimeout(futureTimeoutSeconds, TimeUnit.SECONDS)
                .whenComplete((r, t) -> sample.stop(meterRegistry.timer(
                        "node_fetch_cursor_duration",
                        "groupId", groupId.toString())));
    }


    @Transactional
    public void persistCursor(UUID groupId, UUID domainId, OffsetDateTime createdAt, UUID cursorId) {
        NodesCursorId pk = NodesCursorId.builder().groupId(groupId).domainId(domainId).build();
        NodesCursor cursor = nodesCursorRepository.findById(pk).orElse(new NodesCursor());

        cursor.setId(pk);
        cursor.setCursorCreatedAt(createdAt);
        cursor.setCursorId(cursorId);
        cursor.setUpdatedAt(OffsetDateTime.now());

        nodesCursorRepository.save(cursor);
    }
}