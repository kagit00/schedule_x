package com.shedule.x.service;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.models.Node;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SymmetricGraphBuilderService {
    CompletableFuture<GraphRecords.GraphResult> build(List<Node> nodes, MatchingRequest request);
}