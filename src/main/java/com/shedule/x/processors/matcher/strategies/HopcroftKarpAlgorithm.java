package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.BipartiteGraph;

import java.util.*;

public class HopcroftKarpAlgorithm {

    private final BipartiteGraph graph;
    private final Map<String, String> pairLeft;
    private final Map<String, String> pairRight;
    private final Map<String, Integer> distance;

    public HopcroftKarpAlgorithm(BipartiteGraph graph) {
        this.graph = Objects.requireNonNull(graph, "BipartiteGraph cannot be null.");
        this.pairLeft = new HashMap<>();
        this.pairRight = new HashMap<>();
        this.distance = new HashMap<>();
    }

    public Map<String, String> findMaximumMatching() {
        int matchingSize = 0;
        while (bfs()) {
            for (String leftNode : graph.getLeftNodes()) {
                if (!pairLeft.containsKey(leftNode)) {
                    if (dfs(leftNode)) {
                        matchingSize++;
                    }
                }
            }
        }

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : pairLeft.entrySet()) {
            if (entry.getValue() != null) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private boolean bfs() {
        Queue<String> queue = new LinkedList<>();
        for (String u : graph.getLeftNodes()) {
            if (!pairLeft.containsKey(u)) {
                distance.put(u, 0);
                queue.add(u);
            } else {
                distance.put(u, Integer.MAX_VALUE);
            }
        }

        boolean pathFound = false;
        while (!queue.isEmpty()) {
            String u = queue.poll();
            if (distance.get(u) == Integer.MAX_VALUE) {
                continue;
            }

            for (String v : graph.getNeighbors(u)) {
                String uPrime = pairRight.get(v);
                if (uPrime == null) {
                    pathFound = true;
                } else if (distance.get(uPrime) == Integer.MAX_VALUE) {
                    distance.put(uPrime, distance.get(u) + 1);
                    queue.add(uPrime);
                }
            }
        }
        return pathFound;
    }

    private boolean dfs(String u) {
        if (u == null || distance.get(u) == Integer.MAX_VALUE) {
            return false;
        }

        for (String v : graph.getNeighbors(u)) {
            String uPrime = pairRight.get(v);
            if (uPrime == null || (
                    distance.containsKey(uPrime) &&
                    distance.get(uPrime) == distance.get(u) + 1 &&
                    dfs(uPrime))) {

                pairLeft.put(u, v);
                pairRight.put(v, u);
                return true;
            }
        }
        distance.put(u, Integer.MAX_VALUE);
        return false;
    }
}
