package com.shedule.x.config;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class EdgeBuildingConfig {
    private final int candidateLimit;
    private final double similarityThreshold;
    private final int maxRetries;
    private final long retryDelayMillis;
    private final long chunkTimeoutSeconds;

    public EdgeBuildingConfig(int candidateLimit, double similarityThreshold, int maxRetries,
                              long retryDelayMillis, long chunkTimeoutSeconds) {
        this.candidateLimit = candidateLimit;
        this.similarityThreshold = similarityThreshold;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.chunkTimeoutSeconds = chunkTimeoutSeconds;
    }
}