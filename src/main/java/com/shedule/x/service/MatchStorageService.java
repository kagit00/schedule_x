package com.shedule.x.service;

import com.shedule.x.dto.MatchResult;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface MatchStorageService {
    void saveMatchResults(Map<String, List<MatchResult>> matches, String groupId, UUID domainId);
}
