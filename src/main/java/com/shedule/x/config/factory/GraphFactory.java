package com.shedule.x.config.factory;

import com.shedule.x.dto.NodeDTO;
import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import lombok.experimental.UtilityClass;

import java.util.List;

@UtilityClass
public class GraphFactory {
    public static Graph createSymmetricGraph(List<NodeDTO> nodes) {
        Graph graph = Graph.builder().type(MatchType.SYMMETRIC).build();
        nodes.forEach(graph::addNode);
        return graph;
    }

    public static Graph createBipartiteGraph(List<NodeDTO> leftPartition, List<NodeDTO> rightPartition) {
        Graph graph = Graph.builder().type(MatchType.BIPARTITE).build();
        graph.setLeftPartition(leftPartition);
        graph.setRightPartition(rightPartition);
        leftPartition.forEach(graph::addNode);
        rightPartition.forEach(graph::addNode);
        return graph;
    }

    public static NodeDTO toNodeDTO(Node node) {
        return NodeDTO.builder()
                .id(node.getId())
                .type(node.getType())
                .referenceId(node.getReferenceId())
                .metaData(node.getMetaData())
                .groupId(node.getGroupId())
                .createdAt(node.getCreatedAt())
                .domainId(node.getDomainId())
                .processed(node.isProcessed())
                .build();
    }
}