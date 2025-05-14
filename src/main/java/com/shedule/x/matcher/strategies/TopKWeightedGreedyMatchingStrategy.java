package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.GraphBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;


@Slf4j
@Component("topKWeightedGreedyMatchingStrategy")
public class TopKWeightedGreedyMatchingStrategy implements MatchingStrategy {

    private static final int PARALLELISM_THRESHOLD = 2 * Runtime.getRuntime().availableProcessors() * 1000;

    @Value("${matching.topk.count:100}")
    private int maxMatchesPerNode;

    private final ConcurrentHashMap<String, List<MatchResult>> matches = new ConcurrentHashMap<>();

    @Override
    public Map<String, List<MatchResult>> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.graph();
        if (graph == null || graph.getNodes().isEmpty()) {
            log.warn("Graph is null or empty, returning empty matches");
            return Collections.emptyMap();
        }

        matches.clear();

        Map<String, List<Edge>> adjacencyMap = graph.getAdjacencyMap();
        List<Node> nodes = new ArrayList<>(graph.getNodes());
        Collections.shuffle(nodes);

        selectStream(nodes).forEach(node -> tryMatchNode(node, adjacencyMap));

        log.info("Generated matches for {} nodes for marriage/dating", matches.size());
        return matches;
    }

    private Stream<Node> selectStream(Collection<Node> nodes) {
        return (nodes.size() > PARALLELISM_THRESHOLD) ? nodes.parallelStream() : nodes.stream();
    }

    private void tryMatchNode(Node node, Map<String, List<Edge>> adjacencyMap) {
        String nodeId = node.getReferenceId();

        List<Edge> edges = adjacencyMap.getOrDefault(nodeId, List.of())
                .stream()
                .filter(edge -> !nodeId.equalsIgnoreCase(edge.getToNode().getReferenceId()))
                .sorted(Comparator.comparingDouble(Edge::getWeight).reversed())
                .limit(maxMatchesPerNode)
                .toList();

        if (!edges.isEmpty()) {
            List<MatchResult> nodeMatches = edges.stream()
                    .map(edge -> MatchResult.builder()
                            .partnerId(edge.getToNode().getReferenceId())
                            .score(edge.getWeight())
                            .build())
                    .toList();

            matches.put(nodeId, nodeMatches);
        }
    }

    @Override
    public boolean supports(String mode) {
        return "TopKWeightedGreedyMatchingStrategy".equalsIgnoreCase(mode);
    }
}
