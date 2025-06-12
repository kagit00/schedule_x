package com.shedule.x.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Map;


@Data
@AllArgsConstructor
@Builder
public class CostEdge {
    private int from;
    private int to;
    private int cost;
    private int originalCapacity;
    private int flow;
    private int reverseEdgeIndex;
    private boolean isOriginalProblemEdge;

    public int residualCapacity() {
        return originalCapacity - flow;
    }
}
