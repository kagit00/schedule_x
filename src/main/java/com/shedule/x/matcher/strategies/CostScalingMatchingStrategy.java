package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.*;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;

@Slf4j
@Component("costScalingMatchingStrategy")
public class CostScalingMatchingStrategy implements MatchingStrategy {
    private static final int INITIAL_EPSILON = 1 << 20;

    @Override
    public boolean supports(String mode) {
        return "CostScalingMatchingStrategy".equalsIgnoreCase(mode);
    }

    @Override
    public Map<String, List<MatchResult>> match(GraphRecords.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.getGraph();
        Map<String, List<CostEdge>> residualGraph = buildResidualGraph(graph);
        Map<String, Integer> potential = new HashMap<>();
        Map<String, List<MatchResult>> result = new HashMap<>();

        int epsilon = INITIAL_EPSILON;
        while (epsilon >= 1) {
            dischargeAll(residualGraph, potential);
            epsilon /= 2;
        }

        for (Map.Entry<String, List<CostEdge>> entry : residualGraph.entrySet()) {
            String from = entry.getKey();
            List<MatchResult> matches = new ArrayList<>();
            for (CostEdge edge : entry.getValue()) {
                if (edge.getFlow() == 1 && isLeftToRightEdge(graph, edge.getFrom(), edge.getTo())) {
                    int cost = edge.getCost();
                    String to = edge.getTo();
                    matches.add(new MatchResult(to, -cost));
                }
            }
            if (!matches.isEmpty()) {
                result.put(from, matches);
            }
        }

        return result;
    }

    private void dischargeAll(Map<String, List<CostEdge>> graph, Map<String, Integer> potential) {
        boolean changed;
        do {
            changed = false;
            for (String u : graph.keySet()) {
                for (CostEdge edge : graph.get(u)) {
                    if (edge.residualCapacity() > 0 && edge.reducedCost(potential) < 0) {
                        edge.setFlow(edge.getFlow() + 1);
                        edge.getReverse().setFlow(edge.getReverse().getFlow() - 1);
                        changed = true;
                    }
                }
            }
        } while (changed);
    }

    private Map<String, List<CostEdge>> buildResidualGraph(Graph graph) {
        Map<String, List<CostEdge>> residualGraph = new HashMap<>();

        for (Edge edge : graph.getEdges()) {
            String from = edge.getFromNode().getReferenceId();
            String to = edge.getToNode().getReferenceId();
            int cost = parseCost(edge.getMetaData().get("cost"));

            CostEdge fwd = CostEdge.builder().from(from).to(to).cost(cost).capacity(1).build();
            CostEdge rev = CostEdge.builder().from(to).to(from).cost(-cost).capacity(0).build();

            fwd.setReverse(rev);
            rev.setReverse(fwd);

            residualGraph.computeIfAbsent(from, k -> new ArrayList<>()).add(fwd);
            residualGraph.computeIfAbsent(to, k -> new ArrayList<>()).add(rev);
        }

        return residualGraph;
    }

    private boolean isLeftToRightEdge(Graph graph, String from, String to) {
        return graph.getLeftPartition().stream().anyMatch(n -> n.getReferenceId().equalsIgnoreCase(from)) &&
                graph.getRightPartition().stream().anyMatch(n -> n.getReferenceId().equalsIgnoreCase(to));
    }

    private int parseCost(Object costObj) {
        if (costObj instanceof String) {
            return Integer.parseInt((String) costObj);
        } else if (costObj instanceof Number) {
            return ((Number) costObj).intValue();
        }
        return 1;
    }
}