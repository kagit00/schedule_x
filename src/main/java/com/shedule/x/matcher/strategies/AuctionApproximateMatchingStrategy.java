package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.GraphBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;

@Slf4j
@Component
public class AuctionApproximateMatchingStrategy implements MatchingStrategy {
    private static final double EPSILON = 1e-3;
    private static final double DEFAULT_EDGE_COST = 1.0;

    @Override
    public Map<String, List<MatchResult>> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.graph();
        List<Node> leftNodes = graph.getLeftPartition();
        List<Node> rightNodes = graph.getRightPartition();
        Map<String, String> assignment = new HashMap<>();
        Map<String, Double> price = new HashMap<>();

        for (Node right : rightNodes) {
            price.put(right.getReferenceId(), 0.0);
        }

        for (Node left : leftNodes) {
            String bestRight = null;
            double maxProfit = Double.NEGATIVE_INFINITY;

            for (Edge edge : graph.getEdgesFrom(left)) {
                String rightId = edge.getToNode().getReferenceId();
                double cost = extractEdgeCost(edge);
                double profit = -cost - price.getOrDefault(rightId, 0.0);

                if (profit > maxProfit) {
                    maxProfit = profit;
                    bestRight = rightId;
                }
            }

            if (bestRight != null) {
                assignment.put(left.getReferenceId(), bestRight);
                price.put(bestRight, price.get(bestRight) + EPSILON);
            }
        }

        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : assignment.entrySet()) {
            String leftId = entry.getKey();
            String rightId = entry.getValue();
            double score = -price.getOrDefault(rightId, 0.0);

            result.put(leftId, List.of(new MatchResult(rightId, score)));
        }

        return result;
    }

    @Override
    public boolean supports(String mode) {
        return "AuctionApproximateMatchingStrategy".equalsIgnoreCase(mode);
    }

    private double extractEdgeCost(Edge edge) {
        Object costMeta = edge.getMetaData().getOrDefault("cost", String.valueOf(DEFAULT_EDGE_COST));
        return parseCost(costMeta);
    }

    private double parseCost(Object costObj) {
        if (costObj instanceof String string) {
            return Double.parseDouble(string);
        } else if (costObj instanceof Number number) {
            return number.doubleValue();
        }
        return DEFAULT_EDGE_COST;
    }
}