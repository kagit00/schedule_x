package com.shedule.x.scheduler;


import com.shedule.x.dto.GroupTaskRequest;
import com.shedule.x.models.Domain;
import com.shedule.x.processors.MatchesCreationFinalizer;
import com.shedule.x.processors.PotentialMatchesCreationJobExecutor;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.service.DomainService;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.*;
import com.shedule.x.processors.PotentialMatchComputationProcessor;


import java.util.*;
import java.util.concurrent.*;

@Slf4j
@Component
@Profile("!singleton")
public class PotentialMatchesCreationScheduler {

    private final DomainService domainService;
    private final MatchingGroupRepository matchingGroupRepository;
    private final PotentialMatchesCreationJobExecutor jobExecutor;

    private final PotentialMatchComputationProcessor processor;

    private final MeterRegistry meterRegistry;
    private final Semaphore domainSemaphore;
    private final ConcurrentMap<UUID, CompletableFuture<Void>> groupLocks = new ConcurrentHashMap<>();
    private final ExecutorService batchExecutor;
    private final MatchesCreationFinalizer matchesCreationFinalizer;

    @Value("${match.max-final-batch-size:50000}")
    private int maxFinalBatchSize;

    @Value("${match.save.delay:300000}")
    private long scheduleDelay;

    public PotentialMatchesCreationScheduler(
            DomainService domainService,
            MatchingGroupRepository matchingGroupRepository,
            PotentialMatchesCreationJobExecutor jobExecutor,
            PotentialMatchComputationProcessor processor, // Injected
            MeterRegistry meterRegistry,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            @Value("${match.max-concurrent-domains:2}") int maxConcurrentDomains,
            MatchesCreationFinalizer matchesCreationFinalizer
    ) {
        this.domainService = domainService;
        this.matchingGroupRepository = matchingGroupRepository;
        this.jobExecutor = jobExecutor;
        this.processor = processor;
        this.meterRegistry = meterRegistry;
        this.batchExecutor = batchExecutor;
        this.matchesCreationFinalizer = matchesCreationFinalizer;
        this.domainSemaphore = new Semaphore(maxConcurrentDomains, true);

        validateExecutorPool(batchExecutor, maxConcurrentDomains);
    }

    private void validateExecutorPool(ExecutorService executor, int concurrentDomains) {
        if (executor instanceof ThreadPoolExecutor tpe) {
            int poolSize = tpe.getMaximumPoolSize();
            int requiredSize = concurrentDomains + 2;
            if (poolSize < requiredSize) {
                log.warn("BatchExecutor pool size ({}) is small for maxConcurrentDomains ({}). Potential deadlock.",
                        poolSize, concurrentDomains);
            }
        }
    }

    private void cleanupIdleGroupLocks() {
        groupLocks.entrySet().removeIf(entry -> entry.getValue().isDone());
    }

    @Scheduled(cron = "0 5 11 * * *", zone = "Asia/Kolkata")
    public void processAllDomainsScheduled() {
        Timer.Sample sample = Timer.start(meterRegistry);
        String cycleId = DefaultValuesPopulator.getUid();
        log.info("Starting BATCH MATCHING CYCLE | cycleId={}", cycleId);

        List<Domain> domains = domainService.getActiveDomains();
        if (domains.isEmpty()) {
            log.info("No active domains found. Skipping cycle.");
            sample.stop(meterRegistry.timer("batch_matches_total_duration"));
            return;
        }

        // 1. Flatten all work into a list of (Domain, GroupID) tuples
        List<CompletableFuture<Void>> allTasks = new ArrayList<>();
        List<GroupTaskRequest> tasks = new ArrayList<>();

        for (Domain domain : domains) {
            List<UUID> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            for (UUID groupId : groupIds) {
                tasks.add(new GroupTaskRequest(domain, groupId, cycleId));
            }
        }

        log.info("Found {} groups to process across {} domains.", tasks.size(), domains.size());

        for (GroupTaskRequest task : tasks) {
            allTasks.add(processGroupTask(task));
        }

        CompletableFuture.allOf(allTasks.toArray(new CompletableFuture[0]))
                .orTimeout(3, TimeUnit.HOURS)
                .whenCompleteAsync((v, error) -> {
                    handleCycleCompletion(cycleId, allTasks, error);
                    sample.stop(meterRegistry.timer("batch_matches_total_duration"));
                    cleanupIdleGroupLocks();

                    tasks.forEach(t -> matchesCreationFinalizer.finalize(true));

                }, batchExecutor);
    }


    private CompletableFuture<Void> processGroupTask(GroupTaskRequest request) {
        return groupLocks.compute(request.groupId(), (k, previousTask) -> {
            if (previousTask == null || previousTask.isDone()) {
                return executeGroupTaskChain(request);
            } else {
                log.info("Group {} is busy. Chaining task.", request.groupId());
                return previousTask.thenCompose(v -> executeGroupTaskChain(request));
            }
        });
    }

    private CompletableFuture<Void> executeGroupTaskChain(GroupTaskRequest req) {
        return acquireSemaphoreAsync(domainSemaphore, req.groupId())
                .thenCompose(v -> {
                    log.info("START PROCESSING | groupId={} | domainId={}", req.groupId(), req.domain().getId());

                    // STEP 1: Compute & Ingest (Pushes to QueueManager)
                    return jobExecutor.processGroup(req.groupId(), req.domain().getId(), req.cycleId());
                })
                .thenCompose(v -> {
                    // STEP 2: Drain any remaining items in Queue/Disk to LMDB
                    // The Processor handles the logic of pulling from the QueueManager
                    log.debug("Draining pending matches | groupId={}", req.groupId());
                    return processor.savePendingMatchesAsync(req.groupId(), req.domain().getId(), req.cycleId(), maxFinalBatchSize);
                })
                .thenCompose(v -> {
                    // STEP 3: Streaming Final Save (LMDB -> SQL)
                    log.debug("Executing Final Save | groupId={}", req.groupId());
                    return processor.saveFinalMatches(req.groupId(), req.domain().getId(), req.cycleId(), null, -1);
                })
                .whenCompleteAsync((v, error) -> {
                    // STEP 4: Release & Cleanup (Always runs)
                    domainSemaphore.release();

                    // Close QueueManager and delete spill files
                    processor.cleanup(req.groupId());

                    if (error != null) {
                        log.error("FAILED groupId={} | error={}", req.groupId(), error.getMessage());
                        meterRegistry.counter("matches_creation_error", "groupId", req.groupId().toString()).increment();
                    } else {
                        log.info("SUCCESS groupId={}", req.groupId());
                    }

                    // Notify external finalizer (stats, status updates)
                    matchesCreationFinalizer.finalize(false);
                }, batchExecutor);
    }

    private void handleCycleCompletion(String cycleId, List<CompletableFuture<Void>> tasks, Throwable error) {
        if (error instanceof TimeoutException) {
            log.error("BATCH CYCLE TIMEOUT (3h) | cycleId={}", cycleId);
            tasks.forEach(f -> f.cancel(true));
            meterRegistry.counter("batch_cycle_timeout").increment();
        } else if (error != null) {
            log.error("BATCH CYCLE FAILED | cycleId={}", cycleId, error);
            meterRegistry.counter("batch_cycle_failure").increment();
        } else {
            log.info("BATCH CYCLE COMPLETED | cycleId={}", cycleId);
        }
    }

    private CompletableFuture<Void> acquireSemaphoreAsync(Semaphore semaphore, UUID debugId) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!semaphore.tryAcquire(120, TimeUnit.MINUTES)) {
                    throw new CompletionException(new TimeoutException("Semaphore acquisition timed out for " + debugId));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
        }, batchExecutor);
    }
}