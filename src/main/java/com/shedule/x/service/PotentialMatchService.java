package com.shedule.x.service;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;

import java.util.concurrent.CompletableFuture;

public interface PotentialMatchService {
    CompletableFuture<NodesCount> matchByGroup(MatchingRequest request, int startOffset, int limit, String cycleId);
}