package com.shedule.x.builder;


import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface SymmetricEdgeBuildingStrategy {
    void processBatch(
            List<NodeDTO> sourceNodes,
            List<NodeDTO> targetNodes,
            Collection<GraphRecords.PotentialMatch> matches,
            Set<Edge> edges,
            MatchingRequest request,
            Map<String, Object> context);

    CompletableFuture<Void> indexNodes(List<NodeDTO> nodes, int page);
}