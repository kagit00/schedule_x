package com.shedule.x.service;

import com.shedule.x.dto.MatchResult;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface MatchStorageService {
    CompletableFuture<Void> savePerfectMatchResults(Map<String, List<MatchResult>> results, String groupId, UUID domainId);
}
