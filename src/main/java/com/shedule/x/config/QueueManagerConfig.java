package com.shedule.x.config;

import lombok.Getter;

@Getter
public class QueueManagerConfig {
    private final int capacity;
    private final int flushIntervalSeconds;
    private final double drainWarningThreshold;
    private final int boostBatchFactor;
    private final int maxFinalBatchSize;

    public QueueManagerConfig(
            int capacity,
            int flushIntervalSeconds,
            double drainWarningThreshold,
            int boostBatchFactor,
            int maxFinalBatchSize
    ) {
        this.capacity = capacity;
        this.flushIntervalSeconds = flushIntervalSeconds;
        this.drainWarningThreshold = drainWarningThreshold;
        this.boostBatchFactor = boostBatchFactor;
        this.maxFinalBatchSize = maxFinalBatchSize;
    }
}