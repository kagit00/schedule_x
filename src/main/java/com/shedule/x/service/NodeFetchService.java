package com.shedule.x.service;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;

import com.shedule.x.config.factory.GraphFactory;
import com.shedule.x.dto.CursorPage;
import com.shedule.x.dto.NodeDTO;
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
    private static final int BATCH_OVERLAP = 200;
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

    public CompletableFuture<List<NodeDTO>> fetchNodesInBatchesAsync(
            List<UUID> nodeIds, UUID groupId, LocalDateTime createdAfter) {

        if (nodeIds == null || nodeIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        Timer.Sample sample = Timer.start(meterRegistry);

        return CompletableFuture.supplyAsync(() -> {
                    try {
                        // 1. Fetch JPA Nodes
                        List<Node> nodes = nodeRepository.findByIdsWithMetadata(nodeIds);

                        // 2. Filter in memory
                        List<Node> filteredNodes;
                        if (createdAfter != null) {
                            filteredNodes = nodes.stream()
                                    .filter(n -> n.getCreatedAt() == null || !n.getCreatedAt().isBefore(createdAfter))
                                    .toList();
                        } else {
                            filteredNodes = nodes;
                        }

                        return filteredNodes.stream().map(GraphFactory::toNodeDTO).toList();

                    } catch (Exception e) {
                        log.error("Failed to hydrate nodes for groupId={}", groupId, e);
                        meterRegistry.counter("node_fetch_error", "groupId", groupId.toString()).increment();
                        throw new RuntimeException("Node hydration failed", e);
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


    public CompletableFuture<CursorPage> fetchNodeIdsByCursor(
            UUID groupId, UUID domainId, int limit, String cycleId) {

        Timer.Sample sample = Timer.start(meterRegistry);

        // 1. Calculate the total fetch size needed: Limit from job + Overlap
        final int fetchSize = limit + BATCH_OVERLAP;

        return CompletableFuture.supplyAsync(() -> {
                    // 1. Get Current Cursor State
                    NodesCursor cursor = nodesCursorRepository
                            .findByIdGroupIdAndIdDomainId(groupId, domainId)
                            .orElse(null);

                    LocalDateTime cursorTime = (cursor != null && cursor.getCursorCreatedAt() != null)
                            ? cursor.getCursorCreatedAt().toLocalDateTime() : null;
                    UUID cursorId = (cursor != null) ? cursor.getCursorId() : null;

                    // 2. Fetch Page (ID + CreatedAt) in ONE query, requesting the full overlap amount
                    List<NodeCursorProjection> page = nodeRepository.findUnprocessedNodeIdsAndDatesByCursor(
                            groupId, domainId, cursorTime, cursorId, fetchSize); // Use fetchSize

                    if (page.isEmpty()) {
                        return new CursorPage(Collections.emptyList(), false, null, null);
                    }

                    // 3. Determine the actual list of IDs to process (the "new" chunk)
                    // The 'new' chunk size is the lesser of the requested limit or the total page size
                    int actualProcessLimit = Math.min(limit, page.size());

                    // If the page is smaller than the requested limit, we process all of it.
                    if (page.size() <= limit) {
                        // This is the last batch or a small batch. No overlap needed for the next cycle.
                        log.debug("Fetched smaller batch ({} <= {}), ending cursor retrieval.", page.size(), limit);
                        List<UUID> ids = page.stream().map(NodeCursorProjection::getId).toList();
                        NodeCursorProjection lastItem = page.get(page.size() - 1);

                        // The last item's timestamp is the new cursor position
                        return new CursorPage(ids, false, lastItem.getCreatedAt(), lastItem.getId());
                    }

                    // 4. We fetched >= (limit + overlap), so we have a full page for processing.
                    // The IDs to process are only the first 'limit' nodes.
                    List<UUID> idsToProcess = page.stream()
                            .limit(limit) // <--- Only take the first 'limit' nodes
                            .map(NodeCursorProjection::getId)
                            .toList();

                    // The new cursor position must point to the LAST node of the PROCESSED set (node at index limit - 1)
                    NodeCursorProjection newCursorItem = page.get(limit - 1);

                    log.info("Sliding Window: Fetched {} nodes (Limit={}, Overlap={}). Processing first {} and setting cursor to node {}.",
                            page.size(), limit, BATCH_OVERLAP, idsToProcess.size(), newCursorItem.getId());

                    // isMoreToProcess is true because we know we have the 'overlap' nodes remaining.
                    return new CursorPage(
                            idsToProcess,
                            true,
                            newCursorItem.getCreatedAt(),
                            newCursorItem.getId()
                    );

                }, executor)
                .orTimeout(futureTimeoutSeconds, TimeUnit.SECONDS)
                .whenComplete((r, t) -> sample.stop(meterRegistry.timer(
                        "node_fetch_cursor_duration",
                        "groupId", groupId.toString())));
    }
}