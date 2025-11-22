package com.shedule.x.service;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface PotentialMatchService {
    CompletableFuture<NodesCount> processNodeBatch(List<UUID> nodeIds, MatchingRequest request);
}