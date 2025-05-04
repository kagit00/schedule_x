package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Graph;
import com.shedule.x.service.GraphBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component("noWeightMaximumBipartiteMatchingStrategy")
@Slf4j
public class NoWeightMaximumBipartiteMatchingStrategy implements MatchingStrategy {

    private final HopcroftKarpMatchingStrategy matcher;

    public NoWeightMaximumBipartiteMatchingStrategy(HopcroftKarpMatchingStrategy matcher) {
        this.matcher = matcher;
    }

    @Override
    public Map<String, MatchResult> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.graph();
        Map<String, String> rawMatch = matcher.maximumBipartiteMatch(graph);
        Map<String, MatchResult> result = new HashMap<>();
        for (Map.Entry<String, String> entry : rawMatch.entrySet()) {
            result.put(entry.getKey(), new MatchResult(entry.getValue(), 1.0));
        }
        return result;
    }

    @Override
    public boolean supports(String mode) {
        return "MBM".equalsIgnoreCase(mode);
    }
}
