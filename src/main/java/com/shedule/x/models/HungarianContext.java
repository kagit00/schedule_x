package com.shedule.x.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Arrays;

@Data
@Builder
@AllArgsConstructor
public class HungarianContext {
    private int[] rowPotential;
    private int[] columnPotential;
    private int[] matchColumn;
    private int[] columnPath;
    private int[] minValues;
    private boolean[] visitedColumns;
    private int size;

    public void resetMinValues() {
        Arrays.fill(minValues, Integer.MAX_VALUE);
    }

    public void resetVisited() {
        Arrays.fill(visitedColumns, false);
    }
}
