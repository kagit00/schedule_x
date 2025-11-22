package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.BipartiteGraph;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.BipartiteGraphBuilderService;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component("hopcroftKarpMatchingStrategy")
@Slf4j
public class HopcroftKarpMatchingStrategy implements MatchingStrategy {

    private static final String STRATEGY_MODE = "HopcroftKarpMatchingStrategy";
    private final BipartiteGraphBuilderService bipartiteGraphBuilderService;

    public HopcroftKarpMatchingStrategy(BipartiteGraphBuilderService bipartiteGraphBuilderService) {
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

        log.info("Starting Hopcroft-Karp matching for groupId: {}, domainId: {} with {} potential matches.",
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
            BipartiteGraph graphForAlgorithm = buildBipartiteGraphFromGraphResult(graphResult.getGraph());


            HopcroftKarpAlgorithm algorithm = new HopcroftKarpAlgorithm(graphForAlgorithm);
            Map<String, String> rawMatch = algorithm.findMaximumMatching();

            return mapRawMatchToResult(rawMatch);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Hopcroft-Karp matching interrupted for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Hopcroft-Karp matching interrupted.", e);
        } catch (ExecutionException e) {
            log.error("Graph building failed for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);

            if (e.getCause() instanceof InternalServerErrorException) {
                throw (InternalServerErrorException) e.getCause();
            } else if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            }
            throw new RuntimeException("Failed to obtain graph for Hopcroft-Karp matching.", e);

        } catch (IllegalArgumentException e) {
            log.error("Error preparing graph data for Hopcroft-Karp algorithm for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("An unexpected error occurred during Hopcroft-Karp matching for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Failed to perform Hopcroft-Karp matching.", e);
        }
    }

    private BipartiteGraph buildBipartiteGraphFromGraphResult(Graph graph) {
        if (graph == null || graph.getEdges() == null) {
            throw new IllegalArgumentException("Graph or its edges cannot be null when building for algorithm.");
        }

        Set<String> leftNodes = new HashSet<>();
        Set<String> rightNodes = new HashSet<>();

        for (EdgeDTO edge : graph.getEdges()) {
            leftNodes.add(edge.getFromNodeHash());
            rightNodes.add(edge.getToNodeHash());
        }

        BipartiteGraph bipartiteGraph = new BipartiteGraph(leftNodes, rightNodes);

        for (EdgeDTO edge : graph.getEdges()) {
            bipartiteGraph.addEdge(edge.getFromNodeHash(), edge.getToNodeHash());
        }
        return bipartiteGraph;
    }

    private Map<String, List<MatchResult>> mapRawMatchToResult(Map<String, String> rawMatch) {
        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, String> entry : rawMatch.entrySet()) {
            result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(new MatchResult(entry.getValue(), 1.0));
        }
        return Collections.unmodifiableMap(result);
    }
}
