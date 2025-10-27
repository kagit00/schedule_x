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

    @Scheduled(cron = "0 0 3 * * *", zone = "Asia/Kolkata")
    public void createPerfectMatches() {
        Timer.Sample sample = Timer.start(metrics);
        log.info("Starting Perfect Matches Creation at {}", Instant.now());

        List<Map.Entry<Domain, UUID>> tasks = perfectMatchCreationService.getTasksToProcess();
        if (tasks.isEmpty()) {
            log.info("No groups to process for perfect matches creation");
            sample.stop(metrics.timer("perfect_matches_creation"));
            return;
        }

        tasks.forEach(task -> {
            try {
                generatePerfectMatchesCreationGroup(task.getValue(), task.getKey().getId());
            } catch (Exception e) {
                log.error("Failed to create perfect matches for groupId={}: {}", task.getValue(), e.getMessage());
                metrics.counter("perfect_matches_creation_errors_total", "groupId", task.getValue().toString()).increment();
            }
        });

        sample.stop(metrics.timer("perfect_matches_creation"));
        log.info("Completed Perfect Matches Creation at {}", Instant.now());
    }

    @Retry(name = "perfect_matches")
    @CircuitBreaker(name = "perfect_matches", fallbackMethod = "generatePerfectMatchesCreationGroupFallback")
    private void generatePerfectMatchesCreationGroup(UUID groupId, UUID domainId) {
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
        metrics.counter("perfect_matches_creation", "domainId", domainId.toString(), "groupId", groupId.toString()).increment();

        lastRun.setStatus(JobStatus.COMPLETED.name());
        lastRun.setNodeCount(processedNodes);
        perfectMatchCreationService.saveLastRun(lastRun);

        log.info("Processed storage for groupId={}", groupId);
    }

    private void generatePerfectMatchesCreationGroupFallback(UUID groupId, UUID domainId, Throwable t) {
        metrics.counter("perfect_matches_creation_fallback", "groupId", groupId.toString()).increment();
        LastRunPerfectMatches lastRun = perfectMatchCreationService.getLastRun(domainId, groupId);
        lastRun.setRunDate(LocalDateTime.now());
        lastRun.setStatus(JobStatus.FAILED.name());
        perfectMatchCreationService.saveLastRun(lastRun);
    }
}