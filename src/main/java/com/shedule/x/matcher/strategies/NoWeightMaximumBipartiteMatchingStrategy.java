package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Graph;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("noWeightMaximumBipartiteMatchingStrategy")
@Slf4j
public class NoWeightMaximumBipartiteMatchingStrategy implements MatchingStrategy {

    private final HopcroftKarpMatchingStrategy matcher;

    public NoWeightMaximumBipartiteMatchingStrategy(HopcroftKarpMatchingStrategy matcher) {
        this.matcher = matcher;
    }

    @Override
    public Map<String, List<MatchResult>> match(GraphRecords.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.getGraph();
        Map<String, String> rawMatch = matcher.maximumBipartiteMatch(graph);
        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : rawMatch.entrySet()) {
            result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(new MatchResult(entry.getValue(), 1.0));
        }
        return result;
    }

    @Override
    public boolean supports(String mode) {
        return "MBM".equalsIgnoreCase(mode);
    }
}