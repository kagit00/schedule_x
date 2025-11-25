package com.shedule.x.models;

import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.dto.enums.MatchType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

import java.util.*;


@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Graph {

    private final List<NodeDTO> nodes = new ArrayList<>();
    private final List<EdgeDTO> edges = new ArrayList<>();

    // Internal index for O(1) lookups by Reference ID
    private final Map<String, NodeDTO> nodeMap = new HashMap<>();

    private List<NodeDTO> leftPartition = new ArrayList<>();
    private List<NodeDTO> rightPartition = new ArrayList<>();
    private MatchType type;

    public void addNode(NodeDTO node) {
        nodes.add(node);
        // Maintain the index automatically
        if (node.getReferenceId() != null) {
            nodeMap.put(node.getReferenceId(), node);
        }
    }

    public void addEdge(EdgeDTO edge) {
        edges.add(edge);
    }

    public List<NodeDTO> getNodesByType(String type) {
        if (type == null) return Collections.emptyList();
        return nodes.stream()
                .filter(node -> type.equalsIgnoreCase(node.getType()))
                .collect(Collectors.toList());
    }

    /**
     * Returns edges where the given node is the source.
     * Matches based on ReferenceId <-> FromNodeHash
     */
    public List<EdgeDTO> getEdgesFrom(NodeDTO fromNode) {
        if (fromNode == null || fromNode.getReferenceId() == null) return Collections.emptyList();
        String refId = fromNode.getReferenceId();

        return edges.stream()
                .filter(edge -> refId.equals(edge.getFromNodeHash()))
                .collect(Collectors.toList());
    }

    /**
     * Returns a map of Node Reference ID -> List of Outgoing Edges
     */
    public Map<String, List<EdgeDTO>> getAdjacencyMap() {
        Map<String, List<EdgeDTO>> map = new HashMap<>();
        for (EdgeDTO edge : edges) {
            map.computeIfAbsent(edge.getFromNodeHash(), k -> new ArrayList<>())
                    .add(edge);
        }
        return map;
    }

    /**
     * Returns a map of Node Reference ID -> List of Connected Node Objects.
     * Note: Only includes nodes that exist in this Graph's node list.
     */
    public Map<String, List<NodeDTO>> getAdjacencyList() {
        Map<String, List<NodeDTO>> adjacencyList = new HashMap<>();

        for (EdgeDTO edge : edges) {
            NodeDTO from = nodeMap.get(edge.getFromNodeHash());
            NodeDTO to = nodeMap.get(edge.getToNodeHash());

            // Only map if both nodes are present in this graph context
            if (from != null && to != null) {
                // Forward link
                adjacencyList
                        .computeIfAbsent(from.getReferenceId(), k -> new ArrayList<>())
                        .add(to);

                // Reverse link (assuming undirected graph usage, remove if directed)
                adjacencyList
                        .computeIfAbsent(to.getReferenceId(), k -> new ArrayList<>())
                        .add(from);
            }
        }
        return adjacencyList;
    }
}