package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.*;
import com.shedule.x.service.BipartiteGraphBuilderService;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component("successiveShortestPathStrategy")
@Slf4j
public class SuccessiveShortestPathStrategy implements MatchingStrategy {

    private static final String STRATEGY_MODE = "SuccessiveShortestPath";
    private final BipartiteGraphBuilderService bipartiteGraphBuilderService;

    public SuccessiveShortestPathStrategy(BipartiteGraphBuilderService bipartiteGraphBuilderService) {
        this.bipartiteGraphBuilderService = Objects.requireNonNull(bipartiteGraphBuilderService, "bipartiteGraphBuilderService must not be null");
    }

    @Override
    public boolean supports(String mode) {
        return STRATEGY_MODE.equalsIgnoreCase(mode);
    }

    @Override
    public Map<String, List<MatchResult>> match(List<GraphRecords.PotentialMatch> allPMs, UUID groupId, UUID domainId) {
        if (allPMs == null || allPMs.isEmpty()) {
            log.warn("No potential matches provided for groupId: {} and domainId: {}. Returning empty match.", groupId, domainId);
            return Collections.emptyMap();
        }

        log.info("Starting Successive Shortest Path matching for groupId: {}, domainId: {} with {} potential matches.",
                groupId, domainId, allPMs.size());

        try {
            List<Node> leftNodes = new ArrayList<>();
            List<Node> rightNodes = new ArrayList<>();
            Set<String> uniqueLeftIds = new HashSet<>();
            Set<String> uniqueRightIds = new HashSet<>();

            for (GraphRecords.PotentialMatch pm : allPMs) {
                if (uniqueLeftIds.add(pm.getReferenceId())) {
                    leftNodes.add(Node.builder().referenceId(pm.getReferenceId()).build());
                }
                if (uniqueRightIds.add(pm.getMatchedReferenceId())) {
                    rightNodes.add(Node.builder().referenceId(pm.getMatchedReferenceId()).build());
                }
            }

            MatchingRequest matchingRequest = new MatchingRequest();
            matchingRequest.setGroupId(groupId);
            matchingRequest.setDomainId(domainId);

            CompletableFuture<GraphRecords.GraphResult> graphResultFuture =
                    bipartiteGraphBuilderService.build(leftNodes, rightNodes, matchingRequest);

            GraphRecords.GraphResult graphResult = graphResultFuture.get();

            SuccessiveShortestPathAlgorithm algorithm = new SuccessiveShortestPathAlgorithm(
                    graphResult.getGraph().getLeftPartition(), // Assuming your Graph has getLeftPartition()
                    graphResult.getGraph().getRightPartition(), // Assuming your Graph has getRightPartition()
                    graphResult.getGraph().getEdges()
            );

            Map<String, String> rawAssignment = algorithm.computeMinCostMatching();

            return mapRawMatchToResult(rawAssignment);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Successive Shortest Path matching interrupted for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Successive Shortest Path matching interrupted.", e);
        } catch (ExecutionException e) {
            log.error("Graph building failed for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
            if (e.getCause() instanceof InternalServerErrorException) {
                throw (InternalServerErrorException) e.getCause();
            } else if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            }
            throw new RuntimeException("Failed to obtain graph for Successive Shortest Path matching.", e);
        } catch (IllegalArgumentException e) {
            log.error("Error preparing graph data for Successive Shortest Path algorithm for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("An unexpected error occurred during Successive Shortest Path matching for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Failed to perform Successive Shortest Path matching.", e);
        }
    }

    private Map<String, List<MatchResult>> mapRawMatchToResult(Map<String, String> rawAssignment) {
        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : rawAssignment.entrySet()) {
            result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(new MatchResult(entry.getValue(), 1.0));
        }
        return Collections.unmodifiableMap(result);
    }
}