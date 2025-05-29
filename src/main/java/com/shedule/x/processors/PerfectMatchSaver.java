package com.shedule.x.processors;

import com.shedule.x.models.PerfectMatchEntity;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class PerfectMatchSaver {
    private final PerfectMatchStorageProcessor storageProcessor;
    private final MeterRegistry meterRegistry;
    private final Semaphore saveSemaphore = new Semaphore(2);

    public PerfectMatchSaver(PerfectMatchStorageProcessor storageProcessor, MeterRegistry meterRegistry) {
        this.storageProcessor = storageProcessor;
        this.meterRegistry = meterRegistry;
    }

    @Retryable(
            value = {TimeoutException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public CompletableFuture<Void> saveAsync(List<PerfectMatchEntity> matches, String groupId, UUID domainId) {
        try {
            if (!saveSemaphore.tryAcquire(300, TimeUnit.MILLISECONDS)) {
                log.warn("Timeout acquiring saveSemaphore for groupId={}", groupId);
                meterRegistry.counter("semaphore_acquire_timeout", "groupId", groupId).increment();
                return CompletableFuture.failedFuture(
                        new TimeoutException("Failed to acquire save semaphore within timeout"));
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while acquiring semaphore for groupId={}", groupId, e);
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }

        log.info("Saving {} perfect matches async for groupId={}", matches.size(), groupId);
        return storageProcessor.savePerfectMatches(matches, groupId, domainId)
                .whenComplete((result, ex) -> {
                    saveSemaphore.release();
                    if (ex != null) {
                        meterRegistry.counter("perfect_match_save_errors", "groupId", groupId).increment();
                        log.error("Failed to save {} perfect matches for groupId={}: {}", matches.size(), groupId, ex.getMessage(), ex);
                        // TODO: Publish to dead-letter queue for manual retry
                    } else {
                        log.info("Successfully saved {} perfect matches for groupId={}", matches.size(), groupId);
                    }
                });
    }
}