package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.models.*;
import com.shedule.x.models.Node;
import com.shedule.x.service.BipartiteGraphBuilderService;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;
import com.shedule.x.service.GraphRecords.PotentialMatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


@Component("hungarianMatchingStrategy")
@Slf4j
public class HungarianMatchingStrategy implements MatchingStrategy {

    private static final int MAX_COST = Integer.MAX_VALUE / 4;
    private final BipartiteGraphBuilderService bipartiteGraphBuilderService;

    public HungarianMatchingStrategy(BipartiteGraphBuilderService bipartiteGraphBuilderService) {
        this.bipartiteGraphBuilderService = Objects.requireNonNull(bipartiteGraphBuilderService, "bipartiteGraphBuilderService must not be null");
    }

    @Override
    public Map<String, List<MatchResult>> match(List<PotentialMatch> allPMs, UUID groupId, UUID domainId) {
        if (allPMs == null || allPMs.isEmpty()) {
            log.warn("No potential matches provided for groupId: {} and domainId: {}. Returning empty match.", groupId, domainId);
            return Collections.emptyMap();
        }

        log.info("Starting Hungarian matching for groupId: {}, domainId: {} with {} potential matches.",
                groupId, domainId, allPMs.size());

        try {
            Set<NodeDTO> leftNodes = new HashSet<>();
            Set<NodeDTO> rightNodes = new HashSet<>();

            for (PotentialMatch pm : allPMs) {
                leftNodes.add(NodeDTO.builder().referenceId(pm.getReferenceId()).build());
                rightNodes.add(NodeDTO.builder().referenceId(pm.getMatchedReferenceId()).build());
            }

            MatchingRequest matchingRequest = new MatchingRequest();
            matchingRequest.setGroupId(groupId);
            matchingRequest.setDomainId(domainId);

            CompletableFuture<GraphRecords.GraphResult> graphResultFuture =
                    bipartiteGraphBuilderService.build(new ArrayList<>(leftNodes), new ArrayList<>(rightNodes), matchingRequest);

            GraphRecords.GraphResult graphResult = graphResultFuture.get();
            Graph graph = graphResult.getGraph();

            if (graph.getLeftPartition().isEmpty() || graph.getRightPartition().isEmpty()) {
                log.warn("Graph partitions are empty after building for groupId: {}, domainId: {}. Returning empty match.", groupId, domainId);
                return Collections.emptyMap();
            }

            List<NodeDTO> currentLeftNodes = graph.getLeftPartition();
            List<NodeDTO> currentRightNodes = graph.getRightPartition();

            int n = currentLeftNodes.size();
            int m = currentRightNodes.size();
            int size = Math.max(n, m);

            int[][] costMatrix = initializeCostMatrix(size);

            Map<String, Integer> rightNodeIndex = new HashMap<>();
            for (int j = 0; j < currentRightNodes.size(); j++) {
                rightNodeIndex.put(currentRightNodes.get(j).getReferenceId(), j);
            }

            for (int i = 0; i < n; i++) {
                NodeDTO from = currentLeftNodes.get(i);
                for (EdgeDTO edge : graph.getEdgesFrom(from)) {
                    Integer j = rightNodeIndex.get(edge.getFromNodeHash());
                    if (j != null) {
                        costMatrix[i][j] = (int) -edge.getScore(); // Negate weight for max matching
                    }
                }
            }

            int[] assignment = runHungarianAlgorithm(costMatrix);
            return buildMatchResult(currentLeftNodes, currentRightNodes, costMatrix, assignment, n, m);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Hungarian matching interrupted for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Hungarian matching interrupted.", e);

        } catch (ExecutionException e) {
            log.error("Graph building failed for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
            if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            }
            throw new RuntimeException("Failed to obtain graph for Hungarian matching.", e);

        } catch (IllegalArgumentException e) {
            log.error("Error preparing graph data for Hungarian algorithm for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("An unexpected error occurred during Hungarian matching for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Failed to perform Hungarian matching.", e);
        }
    }

    @Override
    public boolean supports(String mode) {
        return "HUNGARIAN".equalsIgnoreCase(mode);
    }

    private int[][] initializeCostMatrix(int size) {
        int[][] cost = new int[size][size];
        for (int[] row : cost) {
            Arrays.fill(row, MAX_COST);
        }
        return cost;
    }

    private int[] runHungarianAlgorithm(int[][] costMatrix) {
        final int UNMATCHED = 0;
        int size = costMatrix.length;
        HungarianContext ctx = new HungarianContext(size);

        for (int task = 1; task <= size; task++) {
            ctx.getMatchColumn()[UNMATCHED] = task;
            ctx.resetStateForNewAugmentation();
            augmentPath(costMatrix, ctx, UNMATCHED);
        }
        return extractFinalMatches(ctx.getMatchColumn(), size);
    }

    private void augmentPath(int[][] costMatrix, HungarianContext ctx, int currentColumn) {
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

    private int[] extractFinalMatches(int[] matchColumn, int size) {
        int[] matches = new int[size];
        Arrays.fill(matches, -1);
        for (int col = 1; col <= size; col++) {
            if (matchColumn[col] != 0) {
                matches[matchColumn[col] - 1] = col - 1;
            }
        }
        return matches;
    }

    private Map<String, List<MatchResult>> buildMatchResult(
            List<NodeDTO> leftNodes, List<NodeDTO> rightNodes, int[][] costMatrix, int[] assignment, int n, int m) {

        Map<String, List<MatchResult>> result = new HashMap<>();
        for (int i = 0; i < assignment.length; i++) {
            if (i < n && assignment[i] < m && assignment[i] != -1) {
                String fromId = leftNodes.get(i).getReferenceId();
                String toId = rightNodes.get(assignment[i]).getReferenceId();
                double score = -costMatrix[i][assignment[i]];
                result.computeIfAbsent(fromId, k -> new ArrayList<>())
                        .add(new MatchResult(toId, score));
            }
        }
        return result;
    }
}