package com.shedule.x.scheduler;

import com.shedule.x.cache.MatchCache;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.processors.GraphStore;
import com.shedule.x.processors.PotentialMatchStorageProcessor;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class StorageSyncJob {
    private final GraphStore graphStore;
    private final PotentialMatchStorageProcessor storageProcessor;
    private final MatchCache matchCache;
    private final MeterRegistry meterRegistry;

    @Value("${storage.sync.cron:0 0 * * * ?}")
    private String syncCron;

    @Value("${storage.sync.batch-size:500}")
    private int batchSize;

    public StorageSyncJob(
            GraphStore graphStore,
            PotentialMatchStorageProcessor storageProcessor,
            MatchCache matchCache,
            MeterRegistry meterRegistry
    ) {
        this.graphStore = graphStore;
        this.storageProcessor = storageProcessor;
        this.matchCache = matchCache;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(cron = "${storage.sync.cron:0 0 * * * ?}")
    public void syncStorage() {
        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Starting storage sync at {}", DefaultValuesPopulator.getCurrentTimestamp());

        List<String> groupIds = getGroupIds();
        for (String groupId : groupIds) {
            try {
                syncGroup(groupId);
            } catch (Exception e) {
                log.error("Failed to sync groupId={}: {}", groupId, e.getMessage());
                meterRegistry.counter("storage_sync_errors", "groupId", groupId).increment();
            }
        }

        sample.stop(meterRegistry.timer("storage_sync_duration"));
        log.info("Completed storage sync at {}", DefaultValuesPopulator.getCurrentTimestamp());
    }

    @Retry(name = "storageSync")
    @CircuitBreaker(name = "storageSync", fallbackMethod = "syncGroupFallback")
    private void syncGroup(String groupId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        UUID domainId = lookupDomainId(groupId); // Implement lookup
        List<PotentialMatchEntity> batch = new ArrayList<>(batchSize);

        graphStore.streamEdges(domainId, groupId)
                .forEach(edge -> {
                    PotentialMatchEntity match = PotentialMatchEntity.builder()
                            .groupId(groupId)
                            .domainId(domainId)
                            .referenceId(edge.getFromNode().getReferenceId())
                            .matchedReferenceId(edge.getToNode().getReferenceId())
                            .compatibilityScore(edge.getWeight())
                            .matchedAt(DefaultValuesPopulator.getCurrentTimestamp())
                            .build();
                    batch.add(match);
                    if (batch.size() >= batchSize) {
                        //saveBatch(batch, groupId, domainId);
                        batch.clear();
                    }
                });

        if (!batch.isEmpty()) {
            //saveBatch(batch, groupId, domainId);
        }

        matchCache.clearMatches(groupId);
        sample.stop(meterRegistry.timer("storage_sync_group_duration", "groupId", groupId));
        log.info("Synced matches for groupId={}", groupId);
    }

//    private void saveBatch(List<PotentialMatchEntity> batch, String groupId, UUID domainId) {
//        storageProcessor.savePotentialMatches(batch, groupId, domainId)
//                .orTimeout(30, TimeUnit.SECONDS)
//                .whenComplete((result, throwable) -> {
//                    if (throwable != null) {
//                        log.error("Failed to save {} matches for groupId={}: {}", batch.size(), groupId, throwable.getMessage());
//                        meterRegistry.counter("storage_sync_save_errors", "groupId", groupId).increment(batch.size());
//                    } else {
//                        meterRegistry.counter("storage_sync_matches_total", "groupId", groupId).increment(batch.size());
//                        log.info("Synced {} matches for groupId={}", batch.size(), groupId);
//                    }
//                }).join();
//    }

    private void syncGroupFallback(String groupId, Throwable t) {
        log.warn("Sync failed for groupId={}: {}", groupId, t.getMessage());
        meterRegistry.counter("storage_sync_fallbacks", "groupId", groupId).increment();
    }

    private List<String> getGroupIds() {
        // Placeholder: Query DB for groupIds
        return graphStore.listGroupIds(); // Implement in RocksDBGraphStore
    }

    private UUID lookupDomainId(String groupId) {
        // Placeholder: Query DB or cache for domainId
        return UUID.randomUUID(); // Replace with actual lookup
    }
}