package com.shedule.x.dto;

import com.shedule.x.dto.enums.MatchType;

import java.util.Map;
import java.util.UUID;

public class MatchingResponse {
    private Map<UUID, UUID> matches;
    private long durationMs;
    private String strategyUsed;
    private MatchType matchType;
}
