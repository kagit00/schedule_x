package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.service.GraphBuilder;

import java.util.Map;
import java.util.UUID;

public interface MatchingStrategy {
    Map<String, MatchResult> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId);
    boolean supports(String mode);
}
