package com.shedule.x.utils.basic;

import com.shedule.x.dto.NodeDTO;
import com.shedule.x.dto.Snapshot;
import com.shedule.x.dto.enums.State;
import com.shedule.x.processors.LSHIndex;
import com.shedule.x.processors.MetadataEncoder;
import com.shedule.x.service.NodeDataService;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@UtilityClass
public final class IndexUtils {

    public static CompletableFuture<Void> indexNodes(
            // ⚠️ Input is List<NodeDTO> (Correct)
            List<NodeDTO> nodes, int page, LSHIndex lshIndex, MetadataEncoder metadataEncoder,
            Map<UUID, Long> lastModified, AtomicReference<Snapshot> currentSnapshotRef,
            AtomicReference<State> prepState, AtomicReference<CompletableFuture<Boolean>> preparationFuture,
            CompletableFuture<Void> newFuture, int maxRetries, long retryDelayMillis,
            NodeDataService nodeDataService) {

        Map<UUID, int[]> tempEncodedNodesCache = new ConcurrentHashMap<>();
        AtomicInteger invalidCount = new AtomicInteger();
        AtomicInteger duplicatesCount = new AtomicInteger();

        List<Map.Entry<Map<String, String>, UUID>> rawEntries = new ArrayList<>();

        List<NodeDTO> nodesToPersist = new ArrayList<>();

        for (NodeDTO node : nodes) {
            if (node.getId() == null || node.getMetaData() == null || node.getMetaData().isEmpty()) {
                invalidCount.incrementAndGet();
                continue;
            }

            long currentTimestamp = node.getMetaData().getOrDefault("_lastModified", "0").hashCode();
            Long lastTimestamp = lastModified.get(node.getId());

            if (lastTimestamp == null || lastTimestamp != currentTimestamp) {
                rawEntries.add(new AbstractMap.SimpleEntry<>(node.getMetaData(), node.getId()));
                lastModified.put(node.getId(), currentTimestamp);
                nodesToPersist.add(node);
            }
        }

        if (invalidCount.get() > 0) {
            log.warn("Skipped {} nodes with invalid id/metadata during indexing.", invalidCount.get());
        }
        if (duplicatesCount.get() > 0) {
            log.warn("Skipped {} duplicate node IDs during indexing.", duplicatesCount.get());
        }

        UUID groupId = nodes.isEmpty() ? null : nodes.get(0).getGroupId();
        if (groupId != null) {
            for (NodeDTO node : nodesToPersist) {
                nodeDataService.persistNode(node, groupId);
            }
            log.info("Persisted {} new/updated NodeDTOs to disk for groupId={}", nodesToPersist.size(), groupId);
        }

        log.info("Encoding {} new or updated nodes for LSH indexing", rawEntries.size());

        List<Map.Entry<int[], UUID>> encodedEntries = metadataEncoder.encodeBatch(rawEntries);

        if (encodedEntries.isEmpty()) {
            log.warn("No valid encoded entries for indexing, skipping LSH preparation for page={}", page);
            currentSnapshotRef.set(new Snapshot(tempEncodedNodesCache));
            prepState.set(State.SUCCESS);
            preparationFuture.set(CompletableFuture.completedFuture(true));
            newFuture.complete(null);
            return CompletableFuture.completedFuture(null);
        }

        for (Map.Entry<int[], UUID> entry : encodedEntries) {
            tempEncodedNodesCache.put(entry.getValue(), entry.getKey());
        }

        CompletableFuture<Void> indexFuture = lshIndex.insertBatch(encodedEntries);

        return indexFuture.thenRun(() -> {
            if (!Objects.isNull(currentSnapshotRef.get())) {
                currentSnapshotRef.set(new Snapshot(tempEncodedNodesCache));
            }
            nodeDataService.updateEncodedVectorsCache(tempEncodedNodesCache);

            prepState.set(State.SUCCESS);
            preparationFuture.set(CompletableFuture.completedFuture(true));
            log.info("Indexing completed for page={}", page);
            newFuture.complete(null);
        }).exceptionally(e -> {
            prepState.set(State.FAILED);
            log.error("Indexing failed for page={}", page, e);
            newFuture.completeExceptionally(e);
            // Assume InternalServerErrorException exists
            throw new RuntimeException("Indexing failed for page=" + page, e);
        });
    }

    public static boolean ensurePrepared(UUID groupId, int attempt, int maxRetries) {
        // Placeholder for preparation state check
        return true;
    }
}