package com.shedule.x.scheduler;


import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.models.Domain;
import com.shedule.x.models.LastRunPerfectMatches;
import com.shedule.x.service.PerfectMatchCreationService;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
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

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;


@Slf4j
@Component
public class PerfectMatchesCreationScheduler {

    private final MeterRegistry metrics;
    private final PerfectMatchCreationService perfectMatchCreationService;

    @Autowired
    @Lazy
    private PerfectMatchesCreationScheduler self;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public PerfectMatchesCreationScheduler(
            MeterRegistry metrics,
            PerfectMatchCreationService perfectMatchCreationService) {
        this.metrics = metrics;
        this.perfectMatchCreationService = perfectMatchCreationService;
    }

    @Scheduled(cron = "0 20 20 * * *", zone = "Asia/Kolkata")
    public void createPerfectMatches() {
        if (!isSafeToRunPerfectMatches()) {
            log.warn("Skipping perfect matches creation - system not ready or already running");
            return;
        }

        Timer.Sample sample = Timer.start(metrics);
        log.info("Starting Perfect Matches Creation at {}", Instant.now());

        try {
            List<Map.Entry<Domain, UUID>> tasks = perfectMatchCreationService.getTasksToProcess();
            if (tasks.isEmpty()) {
                log.info("No groups eligible for perfect matches creation");
                return;
            }

            for (Map.Entry<Domain, UUID> task : tasks) {
                UUID groupId = task.getValue();
                UUID domainId = task.getKey().getId();
                try {
                    self.processGroupWithResilience(groupId, domainId);
                } catch (Exception e) {
                    log.error("Failed to create perfect matches for groupId={}", groupId, e);
                    metrics.counter("perfect_matches_creation_errors_total", "groupId", groupId.toString()).increment();
                }
            }

            log.info("Completed Perfect Matches Creation successfully at {}", Instant.now());
        } finally {
            running.set(false);
            sample.stop(metrics.timer("perfect_matches_creation"));
        }
    }

    private boolean isSafeToRunPerfectMatches() {
        return running.compareAndSet(false, true);
    }

    @CircuitBreaker(name = "perfectMatchesGroup", fallbackMethod = "processGroupFallback")
    @Retry(name = "perfectMatchesGroup", fallbackMethod = "processGroupFallback")
    @Bulkhead(name = "perfectMatchesGroup", type = Bulkhead.Type.THREADPOOL)
    public void processGroupWithResilience(UUID groupId, UUID domainId) {
        Timer.Sample sample = Timer.start(metrics);

        try {
            LastRunPerfectMatches lastRun = perfectMatchCreationService.getLastRun(domainId, groupId);
            lastRun.setRunDate(LocalDateTime.now());
            lastRun.setStatus(JobStatus.PENDING.name());
            perfectMatchCreationService.saveLastRun(lastRun);

            long processedNodes = perfectMatchCreationService.getProcessedNodeCount(domainId, groupId);
            long lastRunNodeCount = lastRun.getNodeCount();

            log.info("Processing groupId={}, domainId={}. Last run nodes: {}, Current: {}",
                    groupId, domainId, lastRunNodeCount, processedNodes);

            perfectMatchCreationService.processGroup(groupId, domainId);

            lastRun.setStatus(JobStatus.COMPLETED.name());
            lastRun.setNodeCount(processedNodes);
            perfectMatchCreationService.saveLastRun(lastRun);

            metrics.counter("perfect_matches_creation_success_total",
                    "domainId", domainId.toString(),
                    "groupId", groupId.toString()).increment();

        } finally {
            sample.stop(metrics.timer("perfect_matches_group_duration_seconds",
                    "groupId", groupId.toString(),
                    "domainId", domainId.toString()));
        }
    }


    private void processGroupFallback(UUID groupId, UUID domainId, Throwable t) {
        log.error("CircuitBreaker/Retry fallback triggered for groupId={} domainId={}", groupId, domainId, t);

        metrics.counter("perfect_matches_creation_fallback_total",
                "groupId", groupId.toString(),
                "reason", t.getClass().getSimpleName()).increment();

        try {
            LastRunPerfectMatches lastRun = perfectMatchCreationService.getLastRun(domainId, groupId);
            lastRun.setRunDate(LocalDateTime.now());
            lastRun.setStatus(JobStatus.FAILED.name());
            perfectMatchCreationService.saveLastRun(lastRun);
        } catch (Exception ex) {
            log.error("Even fallback failed for groupId={}", groupId, ex);
        }
    }
}