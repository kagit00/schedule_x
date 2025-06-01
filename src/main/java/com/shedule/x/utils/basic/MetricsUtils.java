package com.shedule.x.utils.basic;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@UtilityClass
public final class MetricsUtils {
    public static void registerQueueMetrics(MeterRegistry meterRegistry, String groupId, BlockingQueue<?> queue, int capacity) {
        Gauge.builder("match_queue_size", queue, BlockingQueue::size)
                .tag("groupId", groupId)
                .register(meterRegistry);
        Gauge.builder("match_queue_fill_ratio", queue, q -> (double) q.size() / capacity)
                .tag("groupId", groupId)
                .register(meterRegistry);
    }

    public static void reportQueueMetrics(MeterRegistry meterRegistry, String groupId, AtomicLong enqueueCount,
                                          AtomicLong dequeueCount, ReentrantLock metricsLock) {
        try {
            if (metricsLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                try {
                    meterRegistry.counter("match_queue_enqueued_total", "groupId", groupId)
                            .increment(enqueueCount.getAndSet(0));
                    meterRegistry.counter("match_queue_dequeued_total", "groupId", groupId)
                            .increment(dequeueCount.getAndSet(0));
                } finally {
                    metricsLock.unlock();
                }
            } else {
                log.warn("Could not acquire metricsLock for groupId={}", groupId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while reporting metrics for groupId={}", groupId, e);
        }
    }
}