package com.shedule.x.processors;

import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.models.PotentialMatchEntity;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Component
public class PerfectMatchSaver {
    private final PerfectMatchStorageProcessor storageProcessor;
    private final Semaphore saveSemaphore = new Semaphore(2);
    private volatile boolean shutdownInitiated = false;


    public PerfectMatchSaver(PerfectMatchStorageProcessor storageProcessor) {
        this.storageProcessor = storageProcessor;
    }

    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;
    }


    public CompletableFuture<Void> saveMatchesAsync(List<PerfectMatchEntity> matches, UUID groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Save aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("PerfectMatchSaver is shutting down"));
        }
        log.info("Saving {} matches for groupId={}", matches.size(), groupId);
        CompletableFuture<Void> saveFuture = storageProcessor.savePerfectMatches(matches, groupId, domainId, processingCycleId);
        return saveFuture
                .orTimeout(1_800_000, TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> {
                    log.error("Failed to save matches for groupId={}, processingCycleId={}: {}",
                            groupId, processingCycleId, throwable.getMessage());
                    throw new CompletionException("Save failed", throwable);
                });
    }
}