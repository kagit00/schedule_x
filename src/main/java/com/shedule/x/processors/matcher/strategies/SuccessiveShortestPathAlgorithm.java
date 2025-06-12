package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.FlowEdge;
import com.shedule.x.models.Edge;
import com.shedule.x.models.FibNode;
import com.shedule.x.models.FibonacciHeap;
import com.shedule.x.models.Node;

import java.util.*;
import java.util.stream.Collectors;

public class SuccessiveShortestPathAlgorithm {
    private static final int DEFAULT_EDGE_COST = 1;

    private final List<Node> leftNodes;
    private final List<Node> rightNodes;
    private final List<Edge> initialEdges;

    public SuccessiveShortestPathAlgorithm(List<Node> leftNodes, List<Node> rightNodes, List<Edge> initialEdges) {
        this.leftNodes = Objects.requireNonNull(leftNodes, "Left nodes cannot be null");
        this.rightNodes = Objects.requireNonNull(rightNodes, "Right nodes cannot be null");
        this.initialEdges = Objects.requireNonNull(initialEdges, "Initial edges cannot be null");
    }

    public Map<String, String> computeMinCostMatching() {
        Map<String, List<FlowEdge>> residualGraph = buildResidualGraph();
        Map<String, String> assignment = new HashMap<>();
        Map<String, Integer> potential = new HashMap<>();

        Set<String> leftNodeIds = leftNodes.stream().map(Node::getReferenceId).collect(Collectors.toSet());
        Set<String> rightNodeIds = rightNodes.stream().map(Node::getReferenceId).collect(Collectors.toSet());

        for (String leftNodeId : leftNodeIds) {
            Map<String, Integer> distance = new HashMap<>();
            Map<String, FlowEdge> parent = new HashMap<>();
            findAugmentingPath(leftNodeId, residualGraph, potential, distance);

            String rightMatch = findBestUnmatched(rightNodeIds, assignment, distance);
            if (rightMatch == null) continue;

            updateFlow(leftNodeId, rightMatch, parent);
            assignment.put(rightMatch, leftNodeId);
            updatePotentials(distance, potential);
        }

        return assignment.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    private Map<String, List<FlowEdge>> buildResidualGraph() {
        Map<String, List<FlowEdge>> residualGraph = new HashMap<>();

        Set<String> allNodes = new HashSet<>();
        leftNodes.stream().map(Node::getReferenceId).forEach(allNodes::add);
        rightNodes.stream().map(Node::getReferenceId).forEach(allNodes::add);

        for (Edge edge : initialEdges) {
            String from = edge.getFromNode().getReferenceId();
            String to = edge.getToNode().getReferenceId();
            int cost = parseCost(edge.getMetaData().getOrDefault("cost", String.valueOf(DEFAULT_EDGE_COST)));

            FlowEdge fwd = FlowEdge.builder().from(from).to(to).cost(cost).capacity(1).flow(0).build();
            FlowEdge rev = FlowEdge.builder().from(to).to(from).cost(-cost).capacity(0).flow(0).build();
            fwd.setReverse(rev);
            rev.setReverse(fwd);

            residualGraph.computeIfAbsent(from, k -> new ArrayList<>()).add(fwd);
            residualGraph.computeIfAbsent(to, k -> new ArrayList<>()).add(rev);
        }
        return residualGraph;
    }

    private void findAugmentingPath(String source, Map<String, List<FlowEdge>> residualGraph, Map<String, Integer> potential, Map<String, Integer> distance) {
        FibonacciHeap<String> fibHeap = new FibonacciHeap<>();
        Map<String, FibNode<String>> nodeFibMap = new HashMap<>();

        Set<String> allNodesInGraph = new HashSet<>(residualGraph.keySet());
        for (String node : allNodesInGraph) {
            int dist = node.equals(source) ? 0 : Integer.MAX_VALUE;
            distance.put(node, dist);
            nodeFibMap.put(node, fibHeap.insert(node, dist));
        }

        while (!fibHeap.isEmpty()) {
            String current = fibHeap.extractMin();
            for (FlowEdge edge : residualGraph.getOrDefault(current, Collections.emptyList())) {
                if (edge.residualCapacity() > 0) {
                    int reducedCost = edge.getCost() +
                            potential.getOrDefault(current, 0) -
                            potential.getOrDefault(edge.getTo(), 0);
                    int newDist = distance.get(current) + reducedCost;

                    if (newDist < distance.getOrDefault(edge.getTo(), Integer.MAX_VALUE)) {
                        distance.put(edge.getTo(), newDist);
                        FibNode<String> targetNode = nodeFibMap.get(edge.getTo());
                        if(targetNode != null) {
                            fibHeap.decreaseKey(targetNode, newDist);
                        }
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

    private void updateFlow(String from, String to, Map<String, FlowEdge> parent) {
        String current = to;
        while (!current.equals(from)) {
            FlowEdge edge = parent.get(current);
            edge.setFlow(edge.getFlow() + 1);
            edge.getReverse().setFlow(edge.getReverse().getFlow() - 1);
            current = edge.getFrom();
        }
    }

    private void updatePotentials(Map<String, Integer> dist, Map<String, Integer> potential) {
        for (Map.Entry<String, Integer> entry : dist.entrySet()) {
            if (entry.getValue() != Integer.MAX_VALUE) {
                potential.put(entry.getKey(),
                        potential.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
        }
    }

    private int parseCost(Object costObj) {
        if (costObj instanceof Number) {
            return ((Number) costObj).intValue();
        } else if (costObj instanceof String) {
            try {
                return Integer.parseInt((String) costObj);
            } catch (NumberFormatException e) {
                return DEFAULT_EDGE_COST;
            }
        }
        return DEFAULT_EDGE_COST;
    }
}