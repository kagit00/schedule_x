package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;

@Component("greedySymmetricMatchingStrategy")
@Slf4j
public class GreedySymmetricMatchingStrategy implements MatchingStrategy {

    @Override
    public Map<String, List<MatchResult>> match(GraphRecords.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.getGraph();
        Map<String, List<MatchResult>> matches = new HashMap<>();
        Set<String> visited = new HashSet<>();
        Map<String, List<Node>> adjacencyList = graph.getAdjacencyList();

        for (Node u : graph.getNodes()) {
            String uid = u.getReferenceId();
            if (visited.contains(uid)) continue;

            List<MatchResult> nodeMatches = new ArrayList<>();
            for (Node neighbor : adjacencyList.getOrDefault(uid, List.of())) {
                String nid = neighbor.getReferenceId();
                if (!visited.contains(nid)) {
                    nodeMatches.add(MatchResult.builder().score(1.0).partnerId(nid).build());
                    matches.computeIfAbsent(nid, k -> new ArrayList<>())
                            .add(MatchResult.builder().score(1.0).partnerId(uid).build());
                    visited.add(uid);
                    visited.add(nid);
                    break;
                }
            }
            if (!nodeMatches.isEmpty()) {
                matches.put(uid, nodeMatches);
            }
        }
        return matches;
    }

    @Override
    public boolean supports(String mode) {
        return "GreedySymmetricMatchingStrategy".equalsIgnoreCase(mode);
    }
}