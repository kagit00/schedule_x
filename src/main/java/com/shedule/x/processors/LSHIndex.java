package com.shedule.x.processors;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface LSHIndex {
    CompletableFuture<Void> prepareAsync(List<Map.Entry<int[], UUID>> entries);
    CompletableFuture<Void> insertBatch(List<Map.Entry<int[], UUID>> entries);
    CompletableFuture<Void> insertSingle(int[] metadata, UUID nodeId);
    CompletableFuture<Set<UUID>> queryAsync(int[] metadata, UUID nodeId);
    CompletableFuture<Map<UUID, Set<UUID>>> queryAsyncAll(List<Pair<int[], UUID>> nodeEncodings);
    Set<UUID> querySync(int[] metadata, UUID nodeId);
    long totalBucketsCount();
    boolean isBuilding();
}