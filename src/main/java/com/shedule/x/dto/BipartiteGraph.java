package com.shedule.x.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class BipartiteGraph {
    private Set<String> leftNodes;
    private Set<String> rightNodes;
    private Map<String, List<String>> adjacencyList;

    public BipartiteGraph(Set<String> leftNodes, Set<String> rightNodes) {
        this.leftNodes = Objects.requireNonNull(leftNodes, "Left nodes set cannot be null");
        this.rightNodes = Objects.requireNonNull(rightNodes, "Right nodes set cannot be null");
        this.adjacencyList = new HashMap<>();
        leftNodes.forEach(node -> this.adjacencyList.put(node, new ArrayList<>()));
    }

    public void addEdge(String leftNode, String rightNode) {
        if (!leftNodes.contains(leftNode)) {
            throw new IllegalArgumentException("Left node '" + leftNode + "' is not in the left set.");
        }
        if (!rightNodes.contains(rightNode)) {
            throw new IllegalArgumentException("Right node '" + rightNode + "' is not in the right set.");
        }
        this.adjacencyList.computeIfAbsent(leftNode, k -> new ArrayList<>()).add(rightNode);
    }

    public List<String> getNeighbors(String node) {
        return adjacencyList.getOrDefault(node, Collections.emptyList());
    }

    public Set<String> getLeftNodes() {
        return Collections.unmodifiableSet(leftNodes);
    }

    public Set<String> getRightNodes() {
        return Collections.unmodifiableSet(rightNodes);
    }
}