package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.service.GraphRecords;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface MatchingStrategy {
    Map<String, List<MatchResult>> match(List<GraphRecords.PotentialMatch> allPMs, UUID groupId, UUID domainId);
    boolean supports(String mode);
}
