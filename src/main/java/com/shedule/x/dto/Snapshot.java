package com.shedule.x.dto;

import com.shedule.x.models.Node;

import java.util.Map;
import java.util.UUID;

public record Snapshot(Map<UUID, int[]> encodedNodesCache) {

    /**
     * Creates an immutable snapshot of the current LSH index state.
     * The full Node data is assumed to be persisted on disk (LMDB).
     *
     * @param encodedNodesCache The map of Node ID to its LSH-encoded vector.
     */
    public Snapshot {
        // Enforce immutability for the map passed in
        encodedNodesCache = Map.copyOf(encodedNodesCache);
    }
}