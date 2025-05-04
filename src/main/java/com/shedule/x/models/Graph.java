package com.shedule.x.models;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.enums.NodeType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Graph {

    private final List<Node> nodes = new ArrayList<>();
    private final List<Edge> edges = new ArrayList<>();
    private List<Node> leftPartition = new ArrayList<>();
    private List<Node> rightPartition = new ArrayList<>();
    private MatchType type;

    public void addNode(Node node) {
        nodes.add(node);
    }
    public void addEdge(Edge edge) {
        edges.add(edge);
    }

    public List<Node> getNodesByType(NodeType type) {
        return nodes.stream().filter(node -> node.getType() == type).toList();
    }

    public List<Edge> getEdgesFrom(Node fromNode) {
        return edges.stream().filter(edge -> edge.getFromNode().equals(fromNode)).toList();
    }

    public Map<String, List<Edge>> getAdjacencyMap() {
        Map<String, List<Edge>> map = new HashMap<>();
        for (Edge edge : edges) {
            map.computeIfAbsent(edge.getFromNode().getReferenceId(), k -> new ArrayList<>())
                    .add(edge);
        }
        return map;
    }

    public Map<String, List<Node>> getAdjacencyList() {
        Map<String, List<Node>> adjacencyList = new HashMap<>();
        for (Edge edge : edges) {
            adjacencyList
                    .computeIfAbsent(edge.getFromNode().getReferenceId(), k -> new ArrayList<>())
                    .add(edge.getToNode());

            adjacencyList
                    .computeIfAbsent(edge.getToNode().getReferenceId(), k -> new ArrayList<>())
                    .add(edge.getFromNode());
        }
        return adjacencyList;
    }
}
