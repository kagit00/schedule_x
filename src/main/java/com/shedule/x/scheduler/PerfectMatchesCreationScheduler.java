package com.shedule.x.scheduler;

import com.shedule.x.cache.MatchCache;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.models.Domain;
import com.shedule.x.models.LastRunPerfectMatches;
import com.shedule.x.service.PerfectMatchCreationService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.time.LocalDateTime;


@Slf4j
@Component
public class PerfectMatchesCreationScheduler {
    private final MatchCache cache;
    private final MeterRegistry metrics;
    private final PerfectMatchCreationService perfectMatchCreationService;

    public PerfectMatchesCreationScheduler(
            MatchCache cache,
            MeterRegistry metrics,
            PerfectMatchCreationService perfectMatchCreationService
    ) {
        this.cache = cache;
        this.metrics = metrics;
        this.perfectMatchCreationService = perfectMatchCreationService;
    }

    @Scheduled(cron = "${sync.cron:0 5 16 * * *}")
    public void syncStorage() {
        Timer.Sample sample = Timer.start(metrics);
        log.info("Starting storage sync at {}", Instant.now());

        List<Map.Entry<Domain, UUID>> tasks = perfectMatchCreationService.getTasksToProcess();
        if (tasks.isEmpty()) {
            log.info("No groups to process for storage sync");
            sample.stop(metrics.timer("sync_duration_total"));
            return;
        }

        tasks.forEach(task -> {
            try {
                syncGroup(task.getValue(), task.getKey().getId());
            } catch (Exception e) {
                log.error("Failed to sync groupId={}: {}", task.getValue(), e.getMessage());
                metrics.counter("sync_errors_total", "groupId", task.getValue().toString()).increment();
            }
        });

        sample.stop(metrics.timer("sync_duration_total"));
        log.info("Completed storage sync at {}", Instant.now());
    }

    @Retry(name = "sync")
    @CircuitBreaker(name = "sync", fallbackMethod = "syncGroupFallback")
    private void syncGroup(UUID groupId, UUID domainId) {
        Timer.Sample sample = Timer.start(metrics);
        LastRunPerfectMatches lastRun = perfectMatchCreationService.getLastRun(domainId, groupId);
        lastRun.setRunDate(LocalDateTime.now());
        lastRun.setStatus(JobStatus.PENDING.name());
        perfectMatchCreationService.saveLastRun(lastRun);

        long processedNodes = perfectMatchCreationService.getProcessedNodeCount(domainId, groupId);
        long lastRunNodeCount = lastRun.getNodeCount();

        log.info("Processing groupId={}, domainId={}. Last run nodes: {}, Processed: {}",
                groupId, domainId, lastRunNodeCount, processedNodes);
        cache.clearMatches(groupId);

        perfectMatchCreationService.processAllDomains();
        metrics.counter("sync_triggered_total", "domainId", domainId.toString(), "groupId", groupId.toString()).increment();

        lastRun.setStatus(JobStatus.COMPLETED.name());
        lastRun.setNodeCount(processedNodes);
        perfectMatchCreationService.saveLastRun(lastRun);

        sample.stop(metrics.timer("sync_group_duration", "groupId", groupId.toString()));
        log.info("Processed storage for groupId={}", groupId);
    }

    private void syncGroupFallback(UUID groupId, UUID domainId, Throwable t) {
        log.warn("Sync failed for groupId={}: {}", groupId, t.getMessage());
        metrics.counter("sync_fallbacks_total", "groupId", groupId.toString()).increment();
        LastRunPerfectMatches lastRun = perfectMatchCreationService.getLastRun(domainId, groupId);
        lastRun.setRunDate(LocalDateTime.now());
        lastRun.setStatus(JobStatus.FAILED.name());
        perfectMatchCreationService.saveLastRun(lastRun);
    }
}