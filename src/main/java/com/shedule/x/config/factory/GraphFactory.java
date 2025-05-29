package com.shedule.x.config.factory;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import lombok.experimental.UtilityClass;

import java.util.List;

@UtilityClass
public class GraphFactory {
    public static Graph createSymmetricGraph(List<Node> nodes) {
        Graph graph = Graph.builder().type(MatchType.SYMMETRIC).build();
        nodes.forEach(graph::addNode);
        return graph;
    }

    public static Graph createBipartiteGraph(List<Node> leftPartition, List<Node> rightPartition) {
        Graph graph = Graph.builder().type(MatchType.BIPARTITE).build();
        graph.setLeftPartition(leftPartition);
        graph.setRightPartition(rightPartition);
        leftPartition.forEach(graph::addNode);
        rightPartition.forEach(graph::addNode);
        return graph;
    }
}