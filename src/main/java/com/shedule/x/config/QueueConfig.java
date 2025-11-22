package com.shedule.x.config;

import lombok.Builder;
import lombok.Getter;

import java.util.Objects;
import java.util.UUID;

@Getter
@Builder
public class QueueConfig {
    private final UUID groupId;
    private final UUID domainId;
    private final String processingCycleId;
    private final int capacity;
    private final int flushIntervalSeconds;
    private final double drainWarningThreshold;
    private final int boostBatchFactor;
    private final int maxFinalBatchSize;

    @Builder.Default
    private final boolean useDiskSpill = true;

    public QueueConfig(UUID groupId, UUID domainId, String processingCycleId, int capacity,
                       int flushIntervalSeconds, double drainWarningThreshold, int boostBatchFactor, int maxFinalBatchSize) {
        this(groupId, domainId, processingCycleId, capacity, flushIntervalSeconds,
                drainWarningThreshold, boostBatchFactor, maxFinalBatchSize, true);
    }

    public QueueConfig(UUID groupId, UUID domainId, String processingCycleId, int capacity,
                       int flushIntervalSeconds, double drainWarningThreshold, int boostBatchFactor,
                       int maxFinalBatchSize, boolean useDiskSpill) {
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
        this.domainId = Objects.requireNonNull(domainId, "domainId must not be null");
        this.processingCycleId = Objects.requireNonNull(processingCycleId, "processingCycleId must not be null");
        this.capacity = capacity;
        this.flushIntervalSeconds = flushIntervalSeconds;
        this.drainWarningThreshold = drainWarningThreshold;
        this.boostBatchFactor = boostBatchFactor;
        this.maxFinalBatchSize = maxFinalBatchSize;
        this.useDiskSpill = useDiskSpill;
    }
}