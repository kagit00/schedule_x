package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.service.GraphBuilder;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


public interface AsyncMatchingStrategy {
    CompletableFuture<Map<String, MatchResult>> matchAsync(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId);
}

