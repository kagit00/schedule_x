package com.shedule.x.matcher.strategies;

import com.shedule.x.models.Edge;
import com.shedule.x.models.Node;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.jgrapht.Graph;
import org.jgrapht.alg.matching.blossom.v5.KolmogorovWeightedPerfectMatching;
import org.jgrapht.graph.DefaultUndirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class BlossomMatcher {

    private final List<Node> nodes;
    private final List<Edge> edges;

    public BlossomMatcher(List<Node> nodes, List<Edge> edges) {
        this.nodes = nodes != null ? new ArrayList<>(nodes) : List.of();
        this.edges = edges != null ? new ArrayList<>(edges) : List.of();
    }

    public List<Triple<String, String, Double>> findMaximumWeightMatching() {
        if (nodes.isEmpty() || edges.isEmpty()) {
            log.warn("Empty nodes or edges, throwing exception to trigger fallback");
            throw new IllegalStateException("Empty nodes or edges");
        }

        Graph<String, DefaultWeightedEdge> graph = buildWeightedGraph();
        if (graph.vertexSet().isEmpty()) {
            log.warn("No valid vertices after filtering, throwing exception to trigger fallback");
            throw new IllegalStateException("No valid vertices");
        }

        try {
            KolmogorovWeightedPerfectMatching<String, DefaultWeightedEdge> matcher =
                    new KolmogorovWeightedPerfectMatching<>(graph);
            Set<DefaultWeightedEdge> matching = matcher.getMatching().getEdges();

            List<Triple<String, String, Double>> result = new ArrayList<>(matching.size());
            for (DefaultWeightedEdge e : matching) {
                String u = graph.getEdgeSource(e);
                String v = graph.getEdgeTarget(e);
                double weight = graph.getEdgeWeight(e);
                result.add(Triple.of(u, v, weight));
            }
            log.info("BlossomMatcher produced {} matches", result.size());
            return result;
        } catch (Exception e) {
            log.error("Failed to compute Blossom matching. Graph: vertices={}, edges={}",
                    graph.vertexSet().size(), graph.edgeSet().size(), e);
            throw new RuntimeException("Blossom matching failed", e);
        }
    }

    private Graph<String, DefaultWeightedEdge> buildWeightedGraph() {
        Graph<String, DefaultWeightedEdge> graph =
                new DefaultUndirectedWeightedGraph<>(DefaultWeightedEdge.class);
        Set<String> addedEdges = ConcurrentHashMap.newKeySet();

        nodes.forEach(node -> graph.addVertex(node.getReferenceId()));

        edges.parallelStream()
                .forEach(edge -> {
                    String from = edge.getFromNode().getReferenceId();
                    String to = edge.getToNode().getReferenceId();
                    String key = createEdgeKey(from, to);

                    if (!from.equals(to) && addedEdges.add(key)) {
                        synchronized (graph) {
                            DefaultWeightedEdge weightedEdge = graph.addEdge(from, to);
                            if (weightedEdge != null) {
                                graph.setEdgeWeight(weightedEdge, edge.getWeight());
                            }
                        }
                    }
                });

        return graph;
    }

    private String createEdgeKey(String from, String to) {
        return from.compareTo(to) < 0 ? from + "-" + to : to + "-" + from;
    }
}