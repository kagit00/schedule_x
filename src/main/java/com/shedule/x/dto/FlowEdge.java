package com.shedule.x.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FlowEdge {
    private String from;
    private String to;
    private int capacity;
    private int flow;
    private int cost;
    private FlowEdge reverse;

    public int residualCapacity() {
        return capacity - flow;
    }
}