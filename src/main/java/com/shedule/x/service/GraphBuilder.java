package com.shedule.x.service;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.processors.MetadataCompatibilityCalculator;
import com.shedule.x.utils.graph.WeightFunctionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

@Slf4j
@Service
public class GraphBuilder implements GraphBuilderService {
    private static final double DEFAULT_SCORE = 0.1;

    public record GraphResult(Graph graph, List<PotentialMatch> potentialMatches) {
    }

    public record PotentialMatch(String userId1, String userId2, double compatibilityScore) {
    }

    @Override
    public GraphResult buildSymmetric(List<Node> nodes, String weightFunctionKey, String groupId) {
        Graph graph = Graph.builder().type(MatchType.SYMMETRIC).build();
        nodes.forEach(graph::addNode);

        List<PotentialMatch> potentialMatches = "flat".equalsIgnoreCase(weightFunctionKey)
                ? buildFlatEdges(graph, nodes)
                : buildMetadataEdges(graph, nodes);

        log.debug("Symmetric graph for weightFunctionKey={}: nodes={}, edges={}, potential matches={}",
                weightFunctionKey, graph.getNodes().size(), graph.getEdges().size(), potentialMatches.size());

        return new GraphResult(graph, potentialMatches);
    }

    private List<PotentialMatch> buildFlatEdges(Graph graph, List<Node> nodes) {
        log.debug("Building flat symmetric graph for {} nodes", nodes.size());
        List<PotentialMatch> matches = new ArrayList<>();
        forEachNodePair(nodes, (n1, n2) -> {
            graph.addEdge(Edge.builder().fromNode(n1).toNode(n2).weight(DEFAULT_SCORE).build());
            matches.add(new PotentialMatch(n1.getReferenceId(), n2.getReferenceId(), DEFAULT_SCORE));
        });
        return matches;
    }

    private List<PotentialMatch> buildMetadataEdges(Graph graph, List<Node> nodes) {
        log.debug("Building metadata-based symmetric graph for {} nodes", nodes.size());
        List<PotentialMatch> matches = new ArrayList<>();
        CompatibilityCalculator calculator = new MetadataCompatibilityCalculator();
        forEachNodePair(nodes, (n1, n2) -> {
            double score = calculator.calculate(n1, n2);
            if (score > 0) {
                graph.addEdge(Edge.builder().fromNode(n1).toNode(n2).weight(score).build());
                matches.add(new PotentialMatch(n1.getReferenceId(), n2.getReferenceId(), score));
            }
        });
        return matches;
    }

    private void forEachNodePair(List<Node> nodes, BiConsumer<Node, Node> consumer) {
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                consumer.accept(nodes.get(i), nodes.get(j));
            }
        }
    }

    public GraphResult build(List<Node> leftPartition, List<Node> rightPartition, String weightFunctionKey) {
        Graph graph = Graph.builder().type(MatchType.BIPARTITE).build();
        graph.setLeftPartition(leftPartition);
        graph.setRightPartition(rightPartition);

        var weightFunction = WeightFunctionRegistry.get(weightFunctionKey);
        Set<Edge> edges = ConcurrentHashMap.newKeySet();

        leftPartition.parallelStream().forEach(left -> {
            for (Node right : rightPartition) {
                double weight = weightFunction.apply(left.getMetaData(), right.getMetaData());
                if (weight > 0) {
                    edges.add(Edge.builder()
                            .fromNode(left)
                            .toNode(right)
                            .weight(weight)
                            .metaData(Map.of("weight", String.valueOf(weight)))
                            .build());
                }
            }
        });

        leftPartition.forEach(graph::addNode);
        rightPartition.forEach(graph::addNode);
        edges.forEach(graph::addEdge);

        List<PotentialMatch> potentialMatches = edges.stream()
                .map(edge -> new PotentialMatch(
                        edge.getFromNode().getReferenceId(),
                        edge.getToNode().getReferenceId(),
                        edge.getWeight()))
                .toList();

        return new GraphResult(graph, potentialMatches);
    }
}
