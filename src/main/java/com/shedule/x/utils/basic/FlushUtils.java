package com.shedule.x.utils.basic;

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
    public static void executeFlush(Semaphore semaphore, ExecutorService executor, QueueManagerImpl.QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> callback,
                                    String groupId, UUID domainId, int batchSize, String processingCycleId,
                                    MeterRegistry meterRegistry, AtomicLong lastFlushedQueueSize) {
        try {
            if (!semaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                log.warn("Timeout acquiring semaphore for groupId={}", groupId);
                meterRegistry.counter("semaphore_acquire_timeout", "groupId", groupId).increment();
                return;
            }
            CompletableFuture.runAsync(() ->
                            callback.apply(groupId, domainId, batchSize, processingCycleId)
                                    .orTimeout(60, TimeUnit.SECONDS)
                                    .whenComplete((v, ex) -> {
                                        if (ex != null) {
                                            log.error("Flush failed for groupId={}", groupId, ex);
                                        } else {
                                            lastFlushedQueueSize.set(batchSize);
                                            meterRegistry.counter("graph_builder_flushes", "groupId", groupId).increment();
                                        }
                                    }), executor)
                    .whenComplete((v, e) -> semaphore.release())
                    .exceptionally(e -> {
                        log.error("Flush submission failed for groupId={}", groupId, e);
                        semaphore.release();
                        return null;
                    });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted acquiring semaphore for groupId={}", groupId);
        }
    }

    public static void executeBlockingFlush(Semaphore semaphore, QueueManagerImpl.QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> callback,
                                            String groupId, UUID domainId, int batchSize, String processingCycleId) {
        boolean acquired = false;
        try {
            acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timeout acquiring blockingFlushSemaphore for groupId={}", groupId);
                return;
            }
            callback.apply(groupId, domainId, batchSize, processingCycleId)
                    .orTimeout(30, TimeUnit.SECONDS)
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            log.error("Blocking flush failed for groupId={}", groupId, ex);
                        } else {
                            log.info("Successfully flushed matches for groupId={}", groupId);
                        }
                    }).whenComplete((v, e) -> semaphore.release()).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted acquiring blockingFlushSemaphore for groupId={}", groupId, e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Blocking flush failed or timed out for groupId={}", groupId, e);
        } finally {
            if (acquired) {
                semaphore.release();
            }
        }
    }

    public static void executeBoostedFlush(Semaphore semaphore, ExecutorService executor, QueueManagerImpl.QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> callback,
                                           String groupId, UUID domainId, int batchSize, String processingCycleId, AtomicBoolean boostedDrainInProgress) {
        if (!semaphore.tryAcquire()) {
            log.warn("Failed to acquire boostedFlushSemaphore for groupId={}", groupId);
            boostedDrainInProgress.set(false);
            return;
        }
        try {
            CompletableFuture.runAsync(() ->
                            callback.apply(groupId, domainId, batchSize, processingCycleId)
                                    .orTimeout(30, TimeUnit.SECONDS)
                                    .exceptionally(e -> {
                                        log.error("Boosted drain failed for groupId={}", groupId, e);
                                        return null;
                                    })
                                    .whenComplete((v, e) -> {
                                        boostedDrainInProgress.set(false);
                                        semaphore.release();
                                    }), executor)
                    .exceptionally(e -> {
                        log.error("Boosted drain submission failed for groupId={}", groupId, e);
                        boostedDrainInProgress.set(false);
                        semaphore.release();
                        return null;
                    });
        } catch (Exception e) {
            log.error("Error during boosted drain for groupId={}", groupId, e);
            boostedDrainInProgress.set(false);
            semaphore.release();
        }
    }
}