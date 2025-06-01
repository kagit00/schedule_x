package com.shedule.x.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class GraphStoreConfig {
    private final String dbPath;
    private final int batchSize;
    private final int commitQueueMax;
    private final int commitThreads;

    @Builder.Default
    private static final String DEFAULT_DB_PATH = "e:/web_project/x/graphstore";
    @Builder.Default
    private static final int DEFAULT_BATCH_SIZE = 500;
    @Builder.Default
    private static final int DEFAULT_COMMIT_QUEUE_MAX = 1;
    @Builder.Default
    private static final int DEFAULT_COMMIT_THREADS = 4;

    public static class GraphStoreConfigBuilder {
        private String dbPath = DEFAULT_DB_PATH;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int commitQueueMax = DEFAULT_COMMIT_QUEUE_MAX;
        private int commitThreads = DEFAULT_COMMIT_THREADS;
    }
}