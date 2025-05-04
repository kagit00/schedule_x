package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.GraphBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.stream.Collectors;


@Slf4j
@Component
@RequiredArgsConstructor
public class BlossomSymmetricMatchingStrategy implements MatchingStrategy {

    private static final int BATCH_SIZE = 1000;

    @Override
    public Map<String, MatchResult> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.graph();
        if (graph == null || graph.getNodes().isEmpty()) {
            log.warn("Graph is null or empty, returning empty matches");
            return Collections.emptyMap();
        }

        Map<String, MatchResult> result = new HashMap<>();
        List<List<Node>> nodeBatches = partitionNodes(graph.getNodes());

        for (List<Node> batch : nodeBatches) {
            List<Edge> batchEdges = filterEdgesForBatch(graph.getEdges(), batch);
            BlossomMatcher matcher = new BlossomMatcher(batch, batchEdges);
            List<Triple<String, String, Double>> matches = matcher.findMaximumWeightMatching();

            for (Triple<String, String, Double> match : matches) {
                String u = match.getLeft();
                String v = match.getMiddle();
                double score = match.getRight();
                result.put(u, MatchResult.builder().partnerId(v).score(score).build());
                result.put(v, MatchResult.builder().partnerId(u).score(score).build());
            }
        }

        log.info("BlossomSymmetricMatchingStrategy produced {} matches", result.size() / 2);
        return result;
    }

    private List<List<Node>> partitionNodes(List<Node> nodes) {
        List<List<Node>> batches = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i += BATCH_SIZE) {
            batches.add(nodes.subList(i, Math.min(i + BATCH_SIZE, nodes.size())));
        }
        return batches;
    }

    private List<Edge> filterEdgesForBatch(List<Edge> edges, List<Node> batch) {
        Set<String> batchIds = batch.stream().map(Node::getReferenceId).collect(Collectors.toSet());
        return edges.stream()
                .filter(edge -> batchIds.contains(edge.getFromNode().getReferenceId()) &&
                        batchIds.contains(edge.getToNode().getReferenceId()))
                .toList();
    }

    @Override
    public boolean supports(String mode) {
        return "BlossomSymmetricMatchingStrategy".equalsIgnoreCase(mode);
    }
}

