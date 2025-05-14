package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.service.GraphBuilder;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface MatchingStrategy {
    Map<String, List<MatchResult>> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId);
    boolean supports(String mode);
}
