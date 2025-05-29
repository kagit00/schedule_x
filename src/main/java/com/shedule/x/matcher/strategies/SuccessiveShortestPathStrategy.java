package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.*;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component("successiveShortestPathStrategy")
public class SuccessiveShortestPathStrategy implements MatchingStrategy {
    private static final int DEFAULT_EDGE_COST = 1;

    @Override
    public boolean supports(String mode) {
        return "SuccessiveShortestPath".equalsIgnoreCase(mode);
    }

    @Override
    public Map<String, List<MatchResult>> match(GraphRecords.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.getGraph();
        Map<String, String> rawAssignment = computeMinCostMatching(graph);

        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : rawAssignment.entrySet()) {
            result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(new MatchResult(entry.getValue(), 1.0));
        }

        return result;
    }

    public Map<String, String> computeMinCostMatching(Graph graph) {
        Map<String, List<CostEdge>> residualGraph = buildResidualGraph(graph);
        Map<String, String> assignment = new HashMap<>();
        Map<String, Integer> potential = new HashMap<>();

        Set<String> leftNodeIds = graph.getLeftPartition().stream().map(Node::getReferenceId).collect(Collectors.toSet());
        Set<String> rightNodeIds = graph.getRightPartition().stream().map(Node::getReferenceId).collect(Collectors.toSet());

        for (String leftNode : leftNodeIds) {
            Map<String, Integer> distance = new HashMap<>();
            Map<String, CostEdge> parent = new HashMap<>();
            findAugmentingPath(leftNode, residualGraph, potential, distance, parent);

            String rightMatch = findBestUnmatched(rightNodeIds, assignment, distance);
            if (rightMatch == null) continue;

            updateFlow(leftNode, rightMatch, parent);
            assignment.put(rightMatch, leftNode);
            updatePotentials(distance, potential);
        }

        return assignment.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    private Map<String, List<CostEdge>> buildResidualGraph(Graph graph) {
        Map<String, List<CostEdge>> residualGraph = new HashMap<>();

        for (Edge edge : graph.getEdges()) {
            String from = edge.getFromNode().getReferenceId();
            String to = edge.getToNode().getReferenceId();
            int cost = parseCost(edge.getMetaData().getOrDefault("cost", String.valueOf(DEFAULT_EDGE_COST)));

            CostEdge fwd = CostEdge.builder().from(from).to(to).cost(cost).capacity(1).build();
            CostEdge rev = CostEdge.builder().from(to).to(from).cost(-cost).capacity(0).build();
            fwd.setReverse(rev);
            rev.setReverse(fwd);

            residualGraph.computeIfAbsent(from, k -> new ArrayList<>()).add(fwd);
            residualGraph.computeIfAbsent(to, k -> new ArrayList<>()).add(rev);
        }
        return residualGraph;
    }

    private void findAugmentingPath(String source, Map<String, List<CostEdge>> residualGraph, Map<String, Integer> potential, Map<String, Integer> distance, Map<String, CostEdge> parent) {
        FibonacciHeap<String> fibHeap = new FibonacciHeap<>();
        Map<String, FibNode<String>> nodeMap = new HashMap<>();

        for (String node : residualGraph.keySet()) {
            int dist = node.equals(source) ? 0 : Integer.MAX_VALUE;
            distance.put(node, dist);
            nodeMap.put(node, fibHeap.insert(node, dist));
        }

        while (!fibHeap.isEmpty()) {
            String current = fibHeap.extractMin();
            for (CostEdge edge : residualGraph.getOrDefault(current, List.of())) {
                if (edge.residualCapacity() > 0) {
                    int reducedCost = edge.getCost() +
                            potential.getOrDefault(current, 0) -
                            potential.getOrDefault(edge.getTo(), 0);
                    int newDist = distance.get(current) + reducedCost;

                    if (newDist < distance.getOrDefault(edge.getTo(), Integer.MAX_VALUE)) {
                        distance.put(edge.getTo(), newDist);
                        parent.put(edge.getTo(), edge);
                        fibHeap.decreaseKey(nodeMap.get(edge.getTo()), newDist);
                    }
                }
            }
        }
    }

    private String findBestUnmatched(Set<String> rightNodes, Map<String, String> assignment, Map<String, Integer> distance) {
        String best = null;
        int minCost = Integer.MAX_VALUE;
        for (String rightNode : rightNodes) {
            if (!assignment.containsKey(rightNode)) {
                int dist = distance.getOrDefault(rightNode, Integer.MAX_VALUE);
                if (dist < minCost) {
                    minCost = dist;
                    best = rightNode;
                }
            }
        }
        return best;
    }

    private void updateFlow(String from, String to, Map<String, CostEdge> parent) {
        String current = to;
        while (!current.equals(from)) {
            CostEdge edge = parent.get(current);
            edge.setFlow(edge.getFlow() + 1);
            edge.getReverse().setFlow(edge.getReverse().getFlow() - 1);
            current = edge.getFrom();
        }
    }

    private void updatePotentials(Map<String, Integer> dist, Map<String, Integer> potential) {
        for (Map.Entry<String, Integer> entry : dist.entrySet()) {
            if (entry.getValue() < Integer.MAX_VALUE) {
                potential.put(entry.getKey(),
                        potential.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
        }
    }

    private int parseCost(Object costObj) {
        if (costObj instanceof Number) {
            return ((Number) costObj).intValue();
        } else if (costObj instanceof String) {
            return Integer.parseInt((String) costObj);
        }
        return DEFAULT_EDGE_COST;
    }
}