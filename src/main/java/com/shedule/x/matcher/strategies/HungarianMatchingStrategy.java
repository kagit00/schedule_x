package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.*;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;


@Component("hungarianMatchingStrategy")
@Slf4j
public class HungarianMatchingStrategy implements MatchingStrategy {

    private static final int MAX_COST = Integer.MAX_VALUE / 4;

    @Override
    public Map<String, List<MatchResult>> match(GraphRecords.GraphResult graphResult, String groupId, UUID domainId) {
        Graph graph = graphResult.getGraph();
        List<Node> leftNodes = graph.getLeftPartition();
        List<Node> rightNodes = graph.getRightPartition();

        if (leftNodes.isEmpty() || rightNodes.isEmpty()) {
            return Collections.emptyMap();
        }

        int n = leftNodes.size();
        int m = rightNodes.size();
        int size = Math.max(n, m);

        int[][] cost = new int[size][size];
        for (int[] row : cost) {
            Arrays.fill(row, MAX_COST);
        }

        Map<String, Integer> rightNodeIndex = new HashMap<>();
        for (int j = 0; j < rightNodes.size(); j++) {
            rightNodeIndex.put(rightNodes.get(j).getReferenceId(), j);
        }

        for (int i = 0; i < n; i++) {
            Node from = leftNodes.get(i);
            for (Edge edge : graph.getEdgesFrom(from)) {
                Integer j = rightNodeIndex.get(edge.getToNode().getReferenceId());
                if (j != null) {
                    cost[i][j] = (int) -edge.getWeight();
                }
            }
        }

        int[] assignment = runHungarian(cost);

        Map<String, List<MatchResult>> result = new HashMap<>();
        for (int i = 0; i < assignment.length; i++) {
            if (i < n && assignment[i] < m && assignment[i] != -1) {
                String fromId = leftNodes.get(i).getReferenceId();
                String toId = rightNodes.get(assignment[i]).getReferenceId();
                double score = -cost[i][assignment[i]]; // Reverse cost to score
                result.computeIfAbsent(fromId, k -> new ArrayList<>())
                        .add(new MatchResult(toId, score));
            }
        }

        return result;
    }

    private int[] runHungarian(int[][] costMatrix) {
        final int UNMATCHED = 0;
        int size = costMatrix.length;
        HungarianContext ctx = HungarianContext.builder().size(size).build();

        for (int task = 1; task <= size; task++) {
            ctx.getMatchColumn()[UNMATCHED] = task;
            ctx.resetMinValues();
            ctx.resetVisited();
            augment(costMatrix, ctx, UNMATCHED);
        }

        return extractMatches(ctx.getMatchColumn(), size);
    }

    private void augment(int[][] costMatrix, HungarianContext ctx, int currentColumn) {
        int nextColumn;
        do {
            ctx.getVisitedColumns()[currentColumn] = true;
            int currentTask = ctx.getMatchColumn()[currentColumn];
            int delta = Integer.MAX_VALUE;
            nextColumn = -1;

            for (int col = 1; col <= ctx.getSize(); col++) {
                if (!ctx.getVisitedColumns()[col]) {
                    int reducedCost = costMatrix[currentTask - 1][col - 1]
                            - ctx.getRowPotential()[currentTask]
                            - ctx.getColumnPotential()[col];

                    if (reducedCost < ctx.getMinValues()[col]) {
                        ctx.getMinValues()[col] = reducedCost;
                        ctx.getColumnPath()[col] = currentColumn;
                    }

                    if (ctx.getMinValues()[col] < delta) {
                        delta = ctx.getMinValues()[col];
                        nextColumn = col;
                    }
                }
            }

            updatePotentials(ctx, delta);
            currentColumn = nextColumn;

        } while (ctx.getMatchColumn()[currentColumn] != 0);

        updateMatching(ctx.getColumnPath(), ctx.getMatchColumn(), currentColumn);
    }

    private void updatePotentials(HungarianContext ctx, int delta) {
        for (int col = 0; col <= ctx.getSize(); col++) {
            if (ctx.getVisitedColumns()[col]) {
                ctx.getRowPotential()[ctx.getMatchColumn()[col]] += delta;
                ctx.getColumnPotential()[col] -= delta;
            } else {
                ctx.getMinValues()[col] -= delta;
            }
        }
    }

    private void updateMatching(int[] columnPath, int[] matchColumn, int column) {
        int prev;
        do {
            prev = columnPath[column];
            matchColumn[column] = matchColumn[prev];
            column = prev;
        } while (column != 0);
    }

    private int[] extractMatches(int[] matchColumn, int size) {
        int[] matches = new int[size];
        Arrays.fill(matches, -1);
        for (int col = 1; col <= size; col++) {
            if (matchColumn[col] != 0) {
                matches[matchColumn[col] - 1] = col - 1;
            }
        }
        return matches;
    }

    @Override
    public boolean supports(String mode) {
        return "HUNGARIAN".equalsIgnoreCase(mode);
    }
}