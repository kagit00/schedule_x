package com.shedule.x.service;

import com.shedule.x.dto.NodeDTO;
import com.shedule.x.models.Node;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface NodeDataService {
    void persistNode(NodeDTO node, UUID groupId);
    NodeDTO getNode(UUID nodeId, UUID groupId);
    Map<UUID, int[]> getEncodedVectorsCache(UUID groupId);
    void updateEncodedVectorsCache(Map<UUID, int[]> newVectors);
    void cleanup(UUID groupId);
    NodeDTO getNode(UUID nodeId);
}
