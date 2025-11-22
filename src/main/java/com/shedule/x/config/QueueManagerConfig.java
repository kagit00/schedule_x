package com.shedule.x.config;

public record QueueManagerConfig(int capacity, int flushIntervalSeconds, double drainWarningThreshold,
                                 int boostBatchFactor, int maxFinalBatchSize, boolean useDiskSpill) {
}