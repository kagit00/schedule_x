package com.shedule.x.service;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.models.Node;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface BipartiteGraphBuilderService {
    CompletableFuture<GraphRecords.GraphResult> build(List<Node> leftPartition, List<Node> rightPartition, MatchingRequest request);
}
