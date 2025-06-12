package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.models.*;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.service.BipartiteGraphBuilderService;
import com.shedule.x.service.GraphRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


@Slf4j
@Component("costScalingMatchingStrategy")
public class CostScalingMatchingStrategy implements MatchingStrategy {
    private static final double SCORE_SCALING_FACTOR = 1_000_000.0;

    private final BipartiteGraphBuilderService bipartiteGraphBuilderService;

    private int superSourceNodeId;
    private int superSinkNodeId;
    private Map<String, Integer> stringToNodeIdMap;
    private List<String> nodeIdToStringMap;
    private List<List<CostEdge>> adj;
    private double[] potential;
    private int[] excess;

    public CostScalingMatchingStrategy(BipartiteGraphBuilderService bipartiteGraphBuilderService) {
        this.bipartiteGraphBuilderService = Objects.requireNonNull(bipartiteGraphBuilderService, "bipartiteGraphBuilderService must not be null");
    }

    @Override
    public boolean supports(String mode) {
        return "CostScalingMatchingStrategy".equalsIgnoreCase(mode);
    }

    @Override
    public Map<String, List<MatchResult>> match(List<GraphRecords.PotentialMatch> allPMs, UUID groupId, UUID domainId) {
        if (allPMs == null || allPMs.isEmpty()) {
            log.warn("No potential matches provided for groupId: {} and domainId: {}. Returning empty result.", groupId, domainId);
            return Collections.emptyMap();
        }

        log.info("Starting Cost Scaling matching for groupId: {}, domainId: {} with {} potential matches.",
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
            Graph graph = graphResult.getGraph();

            if (graph.getLeftPartition().isEmpty() || graph.getRightPartition().isEmpty()) {
                log.warn("Graph partitions are empty after building for groupId: {}, domainId: {}. Returning empty match.", groupId, domainId);
                return Collections.emptyMap();
            }

            initializeCostScalingGraphStructures(graph);

            boolean hasLeftToRightEdge = false;
            for (List<CostEdge> edges : adj) {
                for (CostEdge edge : edges) {
                    if (edge.isOriginalProblemEdge() && edge.getFrom() != superSourceNodeId && edge.getTo() != superSinkNodeId) {
                        hasLeftToRightEdge = true;
                        break;
                    }
                }
                if (hasLeftToRightEdge) break;
            }

            if (!hasLeftToRightEdge && superSourceNodeId != -1 && superSinkNodeId != -1) {
                log.debug("No left-to-right edges found in the graph. Returning empty result.");
                return Collections.emptyMap();
            }

            int initialEpsilon = calculateInitialEpsilon();
            log.debug("Starting cost scaling with initial epsilon: {}", initialEpsilon);

            int epsilon = initialEpsilon;
            while (epsilon >= 1) {
                log.debug("Current epsilon: {}", epsilon);
                dischargeAll(epsilon);
                epsilon /= 2;
            }

            return extractMatchingResults();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Cost Scaling matching interrupted for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Cost Scaling matching interrupted.", e);

        } catch (ExecutionException e) {
            log.error("Graph building failed for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
            if (e.getCause() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) e.getCause();
            }
            throw new RuntimeException("Failed to obtain graph for Cost Scaling matching.", e);

        } catch (IllegalArgumentException e) {
            log.error("Error preparing graph data for Cost Scaling algorithm for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage());
            throw e;

        } catch (Exception e) {
            log.error("An unexpected error occurred during Cost Scaling matching for groupId: {}, domainId: {}. Reason: {}",
                    groupId, domainId, e.getMessage(), e);
            throw new RuntimeException("Failed to perform Cost Scaling matching.", e);
        }
    }

    private void initializeCostScalingGraphStructures(Graph graph) {
        Set<String> uniqueNodeStrings = new HashSet<>();
        List<Node> leftPartition = graph.getLeftPartition();
        List<Node> rightPartition = graph.getRightPartition();

        for (Node node : leftPartition) {
            uniqueNodeStrings.add(node.getReferenceId());
        }
        for (Node node : rightPartition) {
            uniqueNodeStrings.add(node.getReferenceId());
        }

        stringToNodeIdMap = new HashMap<>();
        nodeIdToStringMap = new ArrayList<>(uniqueNodeStrings.size() + 2);

        AtomicInteger nextId = new AtomicInteger(0);
        uniqueNodeStrings.forEach(s -> {
            stringToNodeIdMap.put(s, nextId.getAndIncrement());
            nodeIdToStringMap.add(s);
        });

        superSourceNodeId = nextId.getAndIncrement();
        nodeIdToStringMap.add("SUPER_SOURCE");
        superSinkNodeId = nextId.getAndIncrement();
        nodeIdToStringMap.add("SUPER_SINK");

        int numNodes = nextId.get();
        adj = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            adj.add(new ArrayList<>());
        }
        potential = new double[numNodes];
        excess = new int[numNodes];

        for (Node leftNode : leftPartition) {
            int u = stringToNodeIdMap.get(leftNode.getReferenceId());
            addEdge(superSourceNodeId, u, 0); // Edge from super source to left nodes
        }

        for (Node rightNode : rightPartition) {
            int v = stringToNodeIdMap.get(rightNode.getReferenceId());
            addEdge(v, superSinkNodeId, 0); // Edge from right nodes to super sink
        }

        for (Edge graphEdge : graph.getEdges()) {
            int u = stringToNodeIdMap.get(graphEdge.getFromNode().getReferenceId());
            int v = stringToNodeIdMap.get(graphEdge.getToNode().getReferenceId());
            double cost = -graphEdge.getWeight() * SCORE_SCALING_FACTOR; // Negate weight for max cost/min cost
            int integerCost = (int) Math.round(cost);

            if (Math.abs(integerCost) > Integer.MAX_VALUE / 2) {
                log.warn("Scaled cost for edge {} -> {} is very large: {}. Potential overflow risk.",
                        graphEdge.getFromNode().getReferenceId(), graphEdge.getToNode().getReferenceId(), integerCost);
            }

            addEdge(u, v, integerCost); // Original problem edges
        }

        // Initialize excess for the super source
        excess[superSourceNodeId] = leftPartition.size();
    }


    private void addEdge(int from, int to, int cost) {
        CostEdge fwd = CostEdge.builder()
                .from(from)
                .to(to)
                .cost(cost)
                .originalCapacity(1)
                .flow(0)
                .isOriginalProblemEdge(true)
                .build();

        CostEdge rev = CostEdge.builder()
                .from(to)
                .to(from)
                .cost(-cost)
                .originalCapacity(0)
                .flow(0)
                .isOriginalProblemEdge(false)
                .build();

        fwd.setReverseEdgeIndex(adj.get(to).size());
        rev.setReverseEdgeIndex(adj.get(from).size());

        adj.get(from).add(fwd);
        adj.get(to).add(rev);
    }

    private int getResidualCapacity(CostEdge edge) {
        if (edge.isOriginalProblemEdge()) {
            return edge.getOriginalCapacity() - edge.getFlow();
        } else {
            CostEdge forwardCounterpart = adj.get(edge.getTo()).get(edge.getReverseEdgeIndex());
            return forwardCounterpart.getFlow();
        }
    }

    private int calculateInitialEpsilon() {
        int maxCost = 0;
        for (List<CostEdge> edges : adj) {
            for (CostEdge edge : edges) {
                maxCost = Math.max(maxCost, Math.abs(edge.getCost()));
            }
        }
        if (maxCost == 0) return 1;
        return 1 << (32 - Integer.numberOfLeadingZeros(maxCost - 1));
    }


    private void dischargeAll(int epsilon) {
        boolean changed;
        int iterationCount = 0;
        do {
            changed = false;
            iterationCount++;
            int pushesInThisIteration = 0;

            for (int u = 0; u < adj.size(); u++) {
                if (u == superSinkNodeId) continue;

                while (excess[u] > 0) {
                    boolean pushedFromU = false;
                    for (int i = 0; i < adj.get(u).size(); i++) {
                        CostEdge edge = adj.get(u).get(i);
                        int v = edge.getTo();

                        double reducedCost = edge.getCost() + potential[u] - potential[v];
                        int residualCapacity = getResidualCapacity(edge);

                        if (residualCapacity > 0 && reducedCost < -epsilon) {
                            int pushAmount = 1; // For bipartite matching, capacity is 1

                            edge.setFlow(edge.getFlow() + pushAmount);
                            adj.get(v).get(edge.getReverseEdgeIndex()).setFlow(
                                    adj.get(v).get(edge.getReverseEdgeIndex()).getFlow() - pushAmount);

                            excess[u] -= pushAmount;
                            excess[v] += pushAmount;
                            changed = true;
                            pushedFromU = true;
                            pushesInThisIteration++;
                            log.trace("Pushed flow from {} to {} (rc: {}). Excess[{}]: {}, Excess[{}]: {}",
                                    nodeIdToStringMap.get(u), nodeIdToStringMap.get(v), reducedCost,
                                    nodeIdToStringMap.get(u), excess[u], nodeIdToStringMap.get(v), excess[v]);
                            break;
                        }
                    }
                    if (!pushedFromU) break;
                }
            }
            log.debug("  Epsilon {}: Iteration {} - Pushed {} units of flow.", epsilon, iterationCount, pushesInThisIteration);

            if (!changed) {
                int relabelsInThisIteration = 0;
                for (int u = 0; u < adj.size(); u++) {
                    if (excess[u] > 0 && u != superSinkNodeId) {
                        double minReducedCost = Double.POSITIVE_INFINITY;
                        boolean canPush = false;

                        for (CostEdge edge : adj.get(u)) {
                            if (getResidualCapacity(edge) > 0) {
                                int v = edge.getTo();
                                double reducedCost = edge.getCost() + potential[u] - potential[v];
                                if (reducedCost < -epsilon) {
                                    canPush = true; // This path would allow a push
                                    break;
                                }
                                minReducedCost = Math.min(minReducedCost, reducedCost);
                            }
                        }

                        if (!canPush && minReducedCost != Double.POSITIVE_INFINITY) {
                            potential[u] -= (minReducedCost + epsilon);
                            changed = true;
                            relabelsInThisIteration++;
                            log.trace("Relabeled node {} to potential {}. Excess: {}",
                                    nodeIdToStringMap.get(u), potential[u], excess[u]);
                        }
                    }
                }
                log.debug("  Epsilon {}: Iteration {} - Relabeled {} nodes.", epsilon, iterationCount, relabelsInThisIteration);
            }
        } while (changed);
        log.debug("Epsilon {} phase completed in {} iterations.", epsilon, iterationCount);
    }


    private Map<String, List<MatchResult>> extractMatchingResults() {
        Map<String, List<MatchResult>> result = new HashMap<>();
        for (Map.Entry<String, Integer> entry : stringToNodeIdMap.entrySet()) {
            String fromNodeString = entry.getKey();
            int u = entry.getValue();

            if (u == superSourceNodeId || u == superSinkNodeId) {
                continue;
            }

            boolean isLeftNode = false;
            for(CostEdge edge : adj.get(superSourceNodeId)) {
                if(edge.getTo() == u && edge.isOriginalProblemEdge()) {
                    isLeftNode = true;
                    break;
                }
            }

            if (!isLeftNode) continue;

            List<MatchResult> matches = new ArrayList<>();
            for (CostEdge edge : adj.get(u)) {
                if (edge.isOriginalProblemEdge() && edge.getFlow() == 1 &&
                        edge.getFrom() != superSourceNodeId && edge.getTo() != superSinkNodeId) {

                    String toNodeString = nodeIdToStringMap.get(edge.getTo());
                    double originalCompatibilityScore = -edge.getCost() / SCORE_SCALING_FACTOR;
                    matches.add(new MatchResult(toNodeString, originalCompatibilityScore));
                }
            }
            if (!matches.isEmpty()) {
                result.put(fromNodeString, matches);
            }
        }
        return result;
    }

}