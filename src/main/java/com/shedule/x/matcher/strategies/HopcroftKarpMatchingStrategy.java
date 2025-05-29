package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;

@Component("hopcroftKarpMatchingStrategy")
@Slf4j
public class HopcroftKarpMatchingStrategy implements MatchingStrategy {

    @Override
    public boolean supports(String mode) {
        return "HopcroftKarpMatchingStrategy".equalsIgnoreCase(mode);
    }

    @Override
    public Map<String, List<MatchResult>> match(GraphRecords.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.getGraph();
        Map<String, String> rawMatch = maximumBipartiteMatch(graph);
        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : rawMatch.entrySet()) {
            result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(new MatchResult(entry.getValue(), 1.0));
        }
        return result;
    }

    public Map<String, String> maximumBipartiteMatch(Graph graph) {
        if (graph == null || graph.getEdges() == null) {
            throw new IllegalArgumentException("Graph or edges cannot be null.");
        }

        Map<String, List<String>> adjacencyList = buildAdjacencyList(graph);
        Set<String> leftSet = adjacencyList.keySet();
        Set<String> rightSet = new HashSet<>();
        for (List<String> neighbors : adjacencyList.values()) {
            rightSet.addAll(neighbors);
        }

        Map<String, String> pairLeft = new HashMap<>();
        Map<String, String> pairRight = new HashMap<>();
        Map<String, Integer> distance = new HashMap<>();

        while (bfs(adjacencyList, pairLeft, pairRight, distance, leftSet)) {
            for (String left : leftSet) {
                if (!pairLeft.containsKey(left)) {
                    dfs(left, adjacencyList, pairLeft, pairRight, distance);
                }
            }
        }

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : pairLeft.entrySet()) {
            if (entry.getValue() != null) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return Collections.unmodifiableMap(result);
    }

    private Map<String, List<String>> buildAdjacencyList(Graph graph) {
        Map<String, List<String>> adjacencyList = new HashMap<>();
        for (Edge edge : graph.getEdges()) {
            String fromId = edge.getFromNode().getReferenceId();
            String toId = edge.getToNode().getReferenceId();
            adjacencyList.computeIfAbsent(fromId, k -> new ArrayList<>()).add(toId);
        }
        return adjacencyList;
    }

    private boolean bfs(Map<String, List<String>> adjacencyList,
                        Map<String, String> pairLeft,
                        Map<String, String> pairRight,
                        Map<String, Integer> distance,
                        Set<String> leftSet) {

        Queue<String> queue = new LinkedList<>();
        for (String u : leftSet) {
            if (!pairLeft.containsKey(u)) {
                distance.put(u, 0);
                queue.add(u);
            } else {
                distance.put(u, Integer.MAX_VALUE);
            }
        }

        boolean pathFound = false;
        while (!queue.isEmpty()) {
            String u = queue.poll();
            for (String v : adjacencyList.getOrDefault(u, Collections.emptyList())) {
                String uPrime = pairRight.get(v);
                if (uPrime == null) {
                    pathFound = true;
                } else if (distance.get(uPrime) == Integer.MAX_VALUE) {
                    distance.put(uPrime, distance.get(u) + 1);
                    queue.add(uPrime);
                }
            }
        }

        return pathFound;
    }

    private boolean dfs(String u,
                        Map<String, List<String>> adjacencyList,
                        Map<String, String> pairLeft,
                        Map<String, String> pairRight,
                        Map<String, Integer> distance) {

        for (String v : adjacencyList.getOrDefault(u, Collections.emptyList())) {
            String uPrime = pairRight.get(v);
            if (uPrime == null || (
                    distance.get(uPrime) != null &&
                            distance.get(uPrime) == distance.get(u) + 1 &&
                            dfs(uPrime, adjacencyList, pairLeft, pairRight, distance))) {

                pairLeft.put(u, v);
                pairRight.put(v, u);
                return true;
            }
        }

        distance.put(u, Integer.MAX_VALUE);
        return false;
    }
}