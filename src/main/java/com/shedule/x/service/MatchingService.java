package com.shedule.x.service;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;

import java.util.List;
import java.util.Map;

public interface MatchingService {
    Map<String, List<MatchResult>> matchByGroup(MatchingRequest request);
    Map<String, List<MatchResult>> matchByGroup(MatchingRequest request, int page);
}
