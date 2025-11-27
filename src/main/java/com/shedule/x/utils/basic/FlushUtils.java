package com.shedule.x.utils.basic;

import com.shedule.x.config.factory.QuadFunction;
import com.shedule.x.processors.QueueManagerImpl;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@UtilityClass
public final class FlushUtils {

    public static void executeFlush(Semaphore semaphore, ExecutorService executor, QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> callback,
                                    UUID groupId, UUID domainId, int batchSize, String processingCycleId,
                                    MeterRegistry meterRegistry, AtomicLong lastFlushedQueueSize) {
        boolean acquired = false;
        try {
            acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timeout acquiring semaphore for groupId={}", groupId);
                meterRegistry.counter("semaphore_acquire_timeout", "groupId", groupId.toString()).increment();
                return;
            }

            CompletableFuture<Void> flushFuture;
            try {
                flushFuture = callback.apply(groupId, domainId, batchSize, processingCycleId)
                        .orTimeout(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                // callback.apply itself threw — release and return
                log.error("Flush callback invocation error for groupId={}", groupId, e);
                semaphore.release();
                return;
            }

            flushFuture.whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Flush failed for groupId={}", groupId, ex);
                } else {
                    lastFlushedQueueSize.set(batchSize);
                    meterRegistry.counter("graph_builder_flushes", "groupId", groupId.toString()).increment();
                }
                try {
                    semaphore.release();
                } catch (Exception e) {
                    log.error("Error releasing semaphore for groupId={}", groupId, e);
                }
            });

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted acquiring semaphore for groupId={}", groupId, e);
            if (acquired) {
                try { semaphore.release(); } catch (Exception ignore) {}
            }
        }
    }

    public static void executeBlockingFlush(Semaphore semaphore, QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> callback,
                                            UUID groupId, UUID domainId, int batchSize, String processingCycleId) {
        boolean acquired = false;
        try {
            acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timeout acquiring blockingFlushSemaphore for groupId={}", groupId);
                return;
            }

            CompletableFuture<Void> fut;
            try {
                fut = callback.apply(groupId, domainId, batchSize, processingCycleId).orTimeout(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Blocking flush callback invocation failed for groupId={}", groupId, e);
                return;
            }

            try {
                fut.get(30, TimeUnit.SECONDS);
                log.info("Successfully flushed matches for groupId={}", groupId);
            } catch (ExecutionException e) {
                log.error("Blocking flush execution failed for groupId={}", groupId, e.getCause());
            } catch (TimeoutException e) {
                log.error("Blocking flush timed out for groupId={}", groupId, e);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted acquiring blockingFlushSemaphore for groupId={}", groupId, e);
        } finally {
            if (acquired) {
                try { semaphore.release(); } catch (Exception e) {
                    log.error("Error releasing blocking semaphore for groupId={}", groupId, e);
                }
            }
        }
    }

    public static void executeBoostedFlush(Semaphore semaphore, ExecutorService executor, QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> callback,
                                           UUID groupId, UUID domainId, int batchSize, String processingCycleId, AtomicBoolean boostedDrainInProgress) {
        if (!semaphore.tryAcquire()) {
            log.warn("Failed to acquire boostedFlushSemaphore for groupId={}", groupId);
            boostedDrainInProgress.set(false);
            return;
        }
        CompletableFuture<Void> flushFuture = null;
        try {
            try {
                flushFuture = callback.apply(groupId, domainId, batchSize, processingCycleId)
                        .orTimeout(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Boosted flush callback invocation failed for groupId={}", groupId, e);
                boostedDrainInProgress.set(false);
                semaphore.release();
                return;
            }

            flushFuture.whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("Boosted drain failed for groupId={}", groupId, ex);
                }
                boostedDrainInProgress.set(false);
                try { semaphore.release(); } catch (Exception e) { log.error("Error releasing boosted semaphore for groupId={}", groupId, e); }
            });
        } catch (Exception e) {
            log.error("Error during boosted drain for groupId={}", groupId, e);
            boostedDrainInProgress.set(false);
            try { semaphore.release(); } catch (Exception ex) { log.error("Error releasing boosted semaphore after exception for groupId={}", groupId, ex); }
        }
    }
}
