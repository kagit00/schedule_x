package com.shedule.x.builder;


import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.GraphRecords;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface SymmetricEdgeBuildingStrategy {
    void processBatch(List<Node> batch, Graph graph, Collection<GraphRecords.PotentialMatch> matches, Set<Edge> edges, MatchingRequest request, Map<String, Object> context);
    CompletableFuture<Void> indexNodes(List<Node> nodes, int page); // New method
}