package com.shedule.x.dto;

import com.shedule.x.models.Node;

import java.util.Map;
import java.util.UUID;

public record Snapshot(Map<UUID, Node> nodes, Map<UUID, int[]> encodedNodesCache) {
        public Snapshot {
            nodes = Map.copyOf(nodes);
            encodedNodesCache = Map.copyOf(encodedNodesCache);
        }
    }