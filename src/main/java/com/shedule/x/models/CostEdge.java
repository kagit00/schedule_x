package com.shedule.x.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;


@Data
@AllArgsConstructor
@Builder
public class CostEdge {
    private String from;
    private String to;
    private int cost;
    private CostEdge reverse;
    private int capacity;
    private int flow;

    public int residualCapacity() {
        return capacity - flow;
    }
    public int reducedCost(Map<String, Integer> potential) {
        int piU = potential.getOrDefault(from, 0);
        int piV = potential.getOrDefault(to, 0);
        return cost + piU - piV;
    }
}
