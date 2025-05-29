package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("loadBalancingStrategy")
@Slf4j
public class LoadBalancingStrategy implements MatchingStrategy {

    @Override
    public Map<String, List<MatchResult>> match(GraphRecords.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.getGraph();
        Map<String, List<MatchResult>> assignment = new HashMap<>();
        Map<String, Integer> rightNodeLoad = new HashMap<>();

        List<Node> leftNodes = graph.getLeftPartition();
        List<Node> rightNodes = graph.getRightPartition();

        if (leftNodes == null || rightNodes == null || graph.getEdges() == null) {
            log.warn("Graph input is incomplete or corrupted.");
            return Collections.emptyMap();
        }

        for (Node rightNode : rightNodes) {
            rightNodeLoad.put(rightNode.getReferenceId(), 0);
        }

        for (Node leftNode : leftNodes) {
            Node bestRightNode = null;
            int minLoad = Integer.MAX_VALUE;

            for (Edge edge : graph.getEdgesFrom(leftNode)) {
                Node to = edge.getToNode();
                Integer load = rightNodeLoad.get(to.getReferenceId());

                if (load == null) continue;

                if (load < minLoad) {
                    minLoad = load;
                    bestRightNode = to;
                }
            }

            if (bestRightNode != null) {
                log.debug("Assigning node {} to node {} with current load {}", leftNode.getReferenceId(), bestRightNode.getReferenceId(), minLoad);
                assignment.computeIfAbsent(leftNode.getReferenceId(), k -> new ArrayList<>())
                        .add(new MatchResult(bestRightNode.getReferenceId(), minLoad));
                rightNodeLoad.put(bestRightNode.getReferenceId(), minLoad + 1);
            }
        }

        return Collections.unmodifiableMap(assignment);
    }

    @Override
    public boolean supports(String mode) {
        return "LB".equalsIgnoreCase(mode);
    }
}