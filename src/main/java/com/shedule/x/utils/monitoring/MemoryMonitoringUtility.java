package com.shedule.x.utils.monitoring;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@UtilityClass
public final class MemoryMonitoringUtility {
    private static final double MEMORY_THRESHOLD_RATIO = 0.8;
    private static final int BASE_SUB_BATCH_SIZE = 500;
    private final AtomicInteger currentAdjacencyMapSize = new AtomicInteger(0);

    public static boolean isMemoryThresholdExceeded(long maxMemoryMb) {
        Runtime runtime = Runtime.getRuntime();
        long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        long maxMemory = runtime.maxMemory() / (1024 * 1024);

        boolean exceeded = usedMb > (maxMemoryMb * MEMORY_THRESHOLD_RATIO);

        if (exceeded) {
            log.warn("Memory threshold exceeded: used={}MB, maxAllowed={}MB, maxHeap={}MB",
                    usedMb, (long)(maxMemoryMb * MEMORY_THRESHOLD_RATIO), maxMemory);

            log.warn("Memory details - Total: {}MB, Free: {}MB, Used: {}MB",
                    runtime.totalMemory() / (1024 * 1024),
                    runtime.freeMemory() / (1024 * 1024),
                    usedMb);
        }

        return exceeded;
    }

    public static void logMemoryUsage(String context, UUID groupId, long maxMemoryMb) {
        Runtime runtime = Runtime.getRuntime();
        long usedMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        long maxMb = runtime.maxMemory() / (1024 * 1024);
        int utilization = maxMb > 0 ? (int)((usedMb * 100) / maxMb) : 0;

        log.info("Memory usage [{}] for groupId={}: {}MB used, {}MB max, {}% utilization, Active batches: {}",
                context, groupId, usedMb, maxMb, utilization, currentAdjacencyMapSize.get());
    }

    public static int adjustBatchSize(long maxMemoryMb) {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxAllowedMemory = (long) (maxMemoryMb * 1024 * 1024 * MEMORY_THRESHOLD_RATIO);

        if (usedMemory > maxAllowedMemory * 0.7) {
            log.warn("High memory pressure, reducing batch size significantly");
            return Math.max(50, BASE_SUB_BATCH_SIZE / 4);
        } else if (usedMemory > maxAllowedMemory * 0.5) {
            return Math.max(100, BASE_SUB_BATCH_SIZE / 2);
        } else if (usedMemory > maxAllowedMemory * 0.3) {
            return Math.max(200, BASE_SUB_BATCH_SIZE * 3 / 4);
        }

        return BASE_SUB_BATCH_SIZE;
    }
}
