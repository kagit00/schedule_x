package com.shedule.x.utils.basic;

import com.shedule.x.dto.Snapshot;
import com.shedule.x.dto.enums.State;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Node;
import com.shedule.x.processors.LSHIndex;
import com.shedule.x.processors.MetadataEncoder;
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
            List<Node> nodes, int page, LSHIndex lshIndex, MetadataEncoder metadataEncoder,
            Map<UUID, Long> lastModified, AtomicReference<Snapshot> currentSnapshotRef,
            AtomicReference<State> prepState, AtomicReference<CompletableFuture<Boolean>> preparationFuture,
            CompletableFuture<Void> newFuture, int maxRetries, long retryDelayMillis) {

        Map<UUID, Node> tempNodeMap = new ConcurrentHashMap<>();
        Map<UUID, int[]> tempEncodedNodesCache = new ConcurrentHashMap<>();
        AtomicInteger invalidCount = new AtomicInteger();
        AtomicInteger duplicatesCount = new AtomicInteger();
        List<Map.Entry<Map<String, String>, UUID>> rawEntries = new ArrayList<>();

        for (Node node : nodes) {
            if (node.getId() == null || node.getMetaData() == null || node.getMetaData().isEmpty()) {
                invalidCount.incrementAndGet();
                continue;
            }
            if (tempNodeMap.putIfAbsent(node.getId(), node) != null) {
                duplicatesCount.incrementAndGet();
                continue;
            }

            long currentTimestamp = node.getMetaData().getOrDefault("_lastModified", "0").hashCode();
            Long lastTimestamp = lastModified.get(node.getId());
            if (lastTimestamp == null || lastTimestamp != currentTimestamp) {
                rawEntries.add(new AbstractMap.SimpleEntry<>(node.getMetaData(), node.getId()));
                lastModified.put(node.getId(), currentTimestamp);
            }
        }

        if (invalidCount.get() > 0) {
            log.warn("Skipped {} nodes with invalid id/metadata during indexing.", invalidCount.get());
        }
        if (duplicatesCount.get() > 0) {
            log.warn("Skipped {} duplicate node IDs during indexing.", duplicatesCount.get());
        }
        log.info("Encoding {} new or updated nodes for indexing", rawEntries.size());

        List<Map.Entry<int[], UUID>> encodedEntries = metadataEncoder.encodeBatch(rawEntries);
        if (encodedEntries.isEmpty()) {
            log.warn("No valid encoded entries for indexing, skipping LSH preparation for page={}", page);
            currentSnapshotRef.set(new Snapshot(tempNodeMap, tempEncodedNodesCache));
            prepState.set(State.SUCCESS);
            preparationFuture.set(CompletableFuture.completedFuture(true));
            newFuture.complete(null);
            return CompletableFuture.completedFuture(null);
        }

        for (Map.Entry<int[], UUID> entry : encodedEntries) {
            tempEncodedNodesCache.put(entry.getValue(), entry.getKey());
        }

        CompletableFuture<Void> indexFuture = page == 0
                ? lshIndex.prepareAsync(encodedEntries)
                : lshIndex.insertBatch(encodedEntries);

        return indexFuture.thenRun(() -> {
            if (!Objects.isNull(currentSnapshotRef.get())) {
                currentSnapshotRef.set(new Snapshot(tempNodeMap, tempEncodedNodesCache));
            }
            prepState.set(State.SUCCESS);
            preparationFuture.set(CompletableFuture.completedFuture(true));
            log.info("Indexing completed for page={}", page);
            newFuture.complete(null);
        }).exceptionally(e -> {
            prepState.set(State.FAILED);
            log.error("Indexing failed for page={}", page, e);
            newFuture.completeExceptionally(e);
            throw new InternalServerErrorException("Indexing failed for page=" + page);
        });
    }


    public static boolean ensurePrepared(UUID groupId, int attempt, int maxRetries) {
        // Placeholder for preparation state check
        return true;
    }

}