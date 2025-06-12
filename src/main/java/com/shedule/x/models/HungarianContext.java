package com.shedule.x.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.util.Arrays;

@Getter
public class HungarianContext {
    private final int size;
    private final int[] matchColumn;
    private final int[] rowPotential;
    private final int[] columnPotential;
    private final int[] minValues;
    private final int[] columnPath;
    private final boolean[] visitedColumns;

    public HungarianContext(int size) {
        this.size = size;
        this.matchColumn = new int[size + 1];
        this.rowPotential = new int[size + 1];
        this.columnPotential = new int[size + 1];
        this.minValues = new int[size + 1];
        this.columnPath = new int[size + 1];
        this.visitedColumns = new boolean[size + 1];
        resetStateForNewAugmentation();
    }

    public void resetStateForNewAugmentation() {
        Arrays.fill(minValues, Integer.MAX_VALUE / 4);
        Arrays.fill(visitedColumns, false);
        Arrays.fill(columnPath, 0);
    }
}