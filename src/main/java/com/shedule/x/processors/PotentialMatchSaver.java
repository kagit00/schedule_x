package com.shedule.x.processors;

import com.shedule.x.models.PotentialMatchEntity;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class PotentialMatchSaver {
    private final PotentialMatchStorageProcessor storageProcessor;
    private volatile boolean shutdownInitiated = false;

    public PotentialMatchSaver(PotentialMatchStorageProcessor storageProcessor) {
        this.storageProcessor = storageProcessor;
    }

    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;
    }

    public CompletableFuture<Void> saveMatchesAsync(
            List<PotentialMatchEntity> matches, String groupId, UUID domainId, String processingCycleId, boolean finalize) {
        if (shutdownInitiated) {
            log.warn("Save aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("PotentialMatchSaver is shutting down"));
        }
        log.info("Saving {} matches for groupId={}, finalize={}", matches.size(), groupId, finalize);
        CompletableFuture<Void> saveFuture = finalize
                ? storageProcessor.saveAndFinalizeMatches(matches, groupId, domainId, processingCycleId)
                : storageProcessor.savePotentialMatches(matches, groupId, domainId, processingCycleId);
        return saveFuture
                .orTimeout(1_800_000, TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> {
                    log.error("Failed to save matches for groupId={}, processingCycleId={}, finalize={}: {}",
                            groupId, processingCycleId, finalize, throwable.getMessage());
                    throw new CompletionException("Save failed", throwable);
                });
    }

    public long countFinalMatches(String groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Count final matches aborted for groupId={} due to shutdown", groupId);
            throw new IllegalStateException("PotentialMatchSaver is shutting down");
        }
        return storageProcessor.countFinalMatches(groupId, domainId, processingCycleId);
    }

    public CompletableFuture<Void> deleteByGroupId(String groupId) {
        if (shutdownInitiated) {
            log.warn("Delete aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("PotentialMatchSaver is shutting down"));
        }
        return storageProcessor.deleteByGroupId(groupId)
                .exceptionally(throwable -> {
                    log.error("Failed to delete matches for groupId={}: {}", groupId, throwable.getMessage());
                    throw new CompletionException("Delete failed", throwable);
                });
    }
}