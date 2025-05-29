package com.shedule.x.service;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface MatchingService {
    CompletableFuture<NodesCount> matchByGroup(MatchingRequest request, int page, String cycleId);
}