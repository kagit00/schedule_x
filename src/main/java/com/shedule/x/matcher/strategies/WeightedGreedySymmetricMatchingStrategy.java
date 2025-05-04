package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.GraphBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;


@Slf4j
@Component("weightedGreedySymmetricMatchingStrategy")
public class WeightedGreedySymmetricMatchingStrategy implements MatchingStrategy {

    private static final int PARALLELISM_THRESHOLD = 2 * Runtime.getRuntime().availableProcessors() * 1000;

    private final ConcurrentHashMap<String, MatchResult> matches = new ConcurrentHashMap<>();
    private final ConcurrentHashMap.KeySetView<String, Boolean> matched = ConcurrentHashMap.newKeySet();

    @Override
    public Map<String, MatchResult> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.graph();
        if (graph == null || graph.getNodes().isEmpty()) {
            log.warn("Graph is null or empty, returning empty matches");
            return Collections.emptyMap();
        }

        matches.clear();
        matched.clear();

        Map<String, List<Edge>> adjacencyMap = graph.getAdjacencyMap();
        List<Node> nodes = new ArrayList<>(graph.getNodes());
        Collections.shuffle(nodes);

        Stream<Node> nodeStream = selectStream(nodes);
        nodeStream.forEach(node -> tryMatchNode(node, adjacencyMap));

        log.info("Generated {} matches for marriage/dating", matches.size() / 2);
        return matches;
    }

    private Stream<Node> selectStream(Collection<Node> nodes) {
        return nodes.size() > PARALLELISM_THRESHOLD
                ? nodes.parallelStream()
                : nodes.stream();
    }

    private void tryMatchNode(Node node, Map<String, List<Edge>> adjacencyMap) {
        String nodeId = node.getReferenceId();
        if (matched.contains(nodeId)) {
            return;
        }

        List<Edge> edges = adjacencyMap.getOrDefault(nodeId, List.of())
                .stream()
                .filter(edge -> !nodeId.equalsIgnoreCase(edge.getToNode().getReferenceId()))
                .sorted(Comparator.comparingDouble(Edge::getWeight).reversed())
                .toList();

        for (Edge edge : edges) {
            String neighborId = edge.getToNode().getReferenceId();
            if (matched.contains(neighborId)) {
                continue;
            }

            if (matched.add(nodeId)) {
                if (matched.add(neighborId)) {
                    matches.put(nodeId, MatchResult.builder().partnerId(neighborId).score(edge.getWeight()).build());
                    matches.put(neighborId, MatchResult.builder().partnerId(nodeId).score(edge.getWeight()).build());
                    return;
                } else {
                    matched.remove(nodeId);
                }
            }
        }
    }

    @Override
    public boolean supports(String mode) {
        return "WeightedGreedySymmetricMatchingStrategy".equalsIgnoreCase(mode);
    }
}