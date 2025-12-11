package com.shedule.x.service;

import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.models.Domain;
import com.shedule.x.models.LastRunPerfectMatches;
import com.shedule.x.processors.MatchesCreationFinalizer;
import com.shedule.x.processors.PerfectMatchCreationJobExecutor;
import com.shedule.x.repo.DomainRepository;
import com.shedule.x.repo.LastRunPerfectMatchesRepository;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;


import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;


@Slf4j
@Service
public class PerfectMatchCreationService {
    private final MatchingGroupRepository matchingGroupRepository;
    private final PerfectMatchCreationJobExecutor jobExecutor;
    private final MeterRegistry meterRegistry;
    private final Semaphore domainSemaphore;
    private final Semaphore groupSemaphore;
    private final ExecutorService batchExecutor;
    private final MatchesCreationFinalizer matchesCreationFinalizer;
    private final NodeRepository nodeRepository;
    private final LastRunPerfectMatchesRepository lastRunRepository;
    private final DomainService domainService;

    @Value("${match.max-concurrent-domains:2}")
    private int maxConcurrentDomains;

    @Value("${match.max-concurrent-groups:1}")
    private int maxConcurrentGroups;

    @Value("${perfectmatch.node-stability-minutes:10}")
    private long nodeStabilityMinutes;

    private final Map<String, NodeCountSnapshot> nodeCountHistory = new ConcurrentHashMap<>();

    public PerfectMatchCreationService(
            DomainService domainService,
            MatchingGroupRepository matchingGroupRepository,
            PerfectMatchCreationJobExecutor jobExecutor,
            MeterRegistry meterRegistry,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            MatchesCreationFinalizer matchesCreationFinalizer,
            NodeRepository nodeRepository,
            LastRunPerfectMatchesRepository lastRunRepository
    ) {
        this.domainService = domainService;
        this.matchingGroupRepository = matchingGroupRepository;
        this.jobExecutor = jobExecutor;
        this.meterRegistry = meterRegistry;
        this.domainSemaphore = new Semaphore(maxConcurrentDomains, true);
        this.groupSemaphore = new Semaphore(maxConcurrentGroups, true);
        this.batchExecutor = batchExecutor;
        this.matchesCreationFinalizer = matchesCreationFinalizer;
        this.nodeRepository = nodeRepository;
        this.lastRunRepository = lastRunRepository;

        if (batchExecutor instanceof ThreadPoolExecutor tpe) {
            int poolSize = tpe.getMaximumPoolSize();
            int requiredSize = maxConcurrentDomains + maxConcurrentGroups + 2;
            if (poolSize < requiredSize) {
                throw new IllegalArgumentException(
                        "batchExecutor max pool size (" + poolSize + ") must be >= " + requiredSize);
            }
        }
    }

    public List<Map.Entry<Domain, UUID>> getTasksToProcess() {
        List<Domain> domains = domainService.getActiveDomains();
        List<Map.Entry<Domain, UUID>> tasks = new ArrayList<>();

        for (Domain domain : domains) {
            List<UUID> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            for (UUID groupId : groupIds) {
                LastRunPerfectMatches lastRun = getLastRun(domain.getId(), groupId);
                long currentProcessedNodes = getProcessedNodeCount(domain.getId(), groupId);
                long lastRunNodeCount = lastRun.getNodeCount();
                String lastRunStatus = lastRun.getStatus();

                boolean nodeCountIncreased = currentProcessedNodes > lastRunNodeCount;
                boolean lastRunFailedOrPending = JobStatus.PENDING.name().equals(lastRunStatus)
                        || JobStatus.FAILED.name().equals(lastRunStatus);

                boolean isStable = isNodeCountStable(domain.getId(), groupId, currentProcessedNodes);

                boolean shouldProcess = (nodeCountIncreased || lastRunFailedOrPending) && isStable;

                if (shouldProcess) {
                    log.info("Scheduling perfect match for groupId={}, domainId={}. Nodes: {} → {}, stable={}",
                            groupId, domain.getId(), lastRunNodeCount, currentProcessedNodes, isStable);
                    tasks.add(new AbstractMap.SimpleEntry<>(domain, groupId));
                } else {
                    log.debug("Skipping groupId={} domainId={}. Increased={}, Stable={}, Status={}",
                            groupId, domain.getId(), nodeCountIncreased, isStable, lastRunStatus);
                }
            }
        }
        return tasks;
    }

    private boolean isNodeCountStable(UUID domainId, UUID groupId, long currentCount) {
        String key = domainId + ":" + groupId;
        NodeCountSnapshot snapshot = nodeCountHistory.computeIfAbsent(key, k -> new NodeCountSnapshot());

        LocalDateTime now = LocalDateTime.now();
        if (snapshot.count == currentCount &&
                snapshot.timestamp != null &&
                Duration.between(snapshot.timestamp, now).toMinutes() >= nodeStabilityMinutes) {
            return true;
        }

        snapshot.count = currentCount;
        snapshot.timestamp = now;
        return false;
    }

    private static class NodeCountSnapshot {
        long count = -1;
        LocalDateTime timestamp;
    }

    public LastRunPerfectMatches getLastRun(UUID domainId, UUID groupId) {
        return lastRunRepository.findByDomainIdAndGroupId(domainId, groupId)
                .orElseGet(() -> {
                    LastRunPerfectMatches lastRun = new LastRunPerfectMatches();
                    lastRun.setGroupId(groupId);
                    lastRun.setDomainId(domainId);
                    return lastRun;
                });
    }

    public long getProcessedNodeCount(UUID domainId, UUID groupId) {
        return nodeRepository.countByDomainIdAndGroupIdAndProcessedTrue(domainId, groupId);
    }

    public void saveLastRun(LastRunPerfectMatches lastRun) {
        lastRunRepository.save(lastRun);
    }

    public void process() {
        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Starting perfect-match batch at {}", LocalDateTime.now());

        List<Map.Entry<Domain, UUID>> tasks = getTasksToProcess();
        if (tasks.isEmpty()) {
            log.info("No groups to process for perfect matches");
            sample.stop(meterRegistry.timer("batch_perfect_matches_total_duration"));
            return;
        }

        List<CompletableFuture<Void>> batches = new ArrayList<>();
        for (Map.Entry<Domain, UUID> task : tasks) {
            Domain domain = task.getKey();
            UUID groupId = task.getValue();
            batches.add(processGroupTask(groupId, domain.getId()));
        }

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(batches.toArray(new CompletableFuture[0]))
                .thenAcceptAsync(v -> {
                    log.info("Completed perfect-match batch at {}",
                            LocalDateTime.now());
                    resultFuture.complete(null);
                }, batchExecutor)
                .whenComplete((v, t) -> matchesCreationFinalizer.finalize(true))
                .exceptionally(t -> {
                    log.error("Perfect-match batch processing failed: {}", t.getMessage(), t);
                    resultFuture.completeExceptionally(t);
                    return null;
                });

        try {
            resultFuture.get(60, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("Error waiting for perfect-match batch completion {}", e.getMessage(), e);
        }

        sample.stop(meterRegistry.timer("batch_perfect_matches_total_duration"));
    }

    private CompletableFuture<Void> processGroupTask(UUID groupId, UUID domainId) {
        Timer.Sample batchTimer = Timer.start(meterRegistry);

        LastRunPerfectMatches lastRun = getLastRun(domainId, groupId);
        lastRun.setRunDate(LocalDateTime.now());
        lastRun.setStatus(JobStatus.PENDING.name());
        saveLastRun(lastRun);

        return CompletableFuture.runAsync(() -> {
            log.info("Attempting to acquire domainSemaphore for groupId={} domainId={}, queueLength={}",
                    groupId, domainId, domainSemaphore.getQueueLength());
            try {
                boolean acquired = domainSemaphore.tryAcquire(3, TimeUnit.MINUTES);
                if (!acquired) {
                    throw new TimeoutException("Timed out acquiring domainSemaphore for domainId=" + domainId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted acquiring domainSemaphore", e);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }, batchExecutor).thenCompose(v -> CompletableFuture.runAsync(() -> {
            log.info("Attempting to acquire groupSemaphore for groupId={} domainId={}, queueLength={}",
                    groupId, domainId, groupSemaphore.getQueueLength());
            try {
                boolean acquired = groupSemaphore.tryAcquire(240, TimeUnit.MINUTES);
                if (!acquired) {
                    throw new TimeoutException("Timed out acquiring groupSemaphore for groupId=" + groupId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted acquiring groupSemaphore", e);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }, batchExecutor)).thenCompose(v -> {
            log.info("Acquired locks for perfect match: groupId={} domainId={}", groupId, domainId);
            Timer.Sample taskTimer = Timer.start(meterRegistry);
            return jobExecutor.processGroup(groupId, domainId)
                    .thenRunAsync(() -> {
                        doFlushLoop(groupId, domainId);
                        long processedNodes = getProcessedNodeCount(domainId, groupId);
                        lastRun.setNodeCount(processedNodes);
                        lastRun.setStatus(JobStatus.COMPLETED.name());
                        saveLastRun(lastRun);
                        taskTimer.stop(meterRegistry.timer("task_processing_duration",
                                "domainId", domainId.toString(), "groupId", groupId.toString()));
                    }, batchExecutor);
        }).whenComplete((v, e) -> {
            groupSemaphore.release();
            domainSemaphore.release();
            log.info("Released locks for perfect match: groupId={} domainId={}, permits: domain={} group={}",
                    groupId, domainId, domainSemaphore.availablePermits(), groupSemaphore.availablePermits());

            if (e != null) {
                meterRegistry.counter("matches_creation_error", "mode", "perfect",
                        "domainId", domainId.toString(), "groupId", groupId.toString()).increment();
                log.error("Error in perfect-match batch+flush for groupId={} domainId={}: {}",
                        groupId, domainId, e.getMessage(), e);
                lastRun.setStatus(JobStatus.FAILED.name());
                saveLastRun(lastRun);
            }

            batchTimer.stop(meterRegistry.timer("batch_perfect_matches_duration",
                    "domainId", domainId.toString(), "groupId", groupId.toString()));
        });
    }

    private void doFlushLoop(UUID groupId, UUID domainId) {
        // Implement flush logic if needed
    }

    public void processGroup(UUID groupId, UUID domainId) {
        processGroupTask(groupId, domainId).join();
    }
}