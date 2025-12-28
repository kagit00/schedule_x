package com.shedule.x.scheduler;


import com.shedule.x.dto.GroupTaskRequest;
import com.shedule.x.models.Domain;
import com.shedule.x.processors.MatchesCreationFinalizer;
import com.shedule.x.processors.PotentialMatchesCreationJobExecutor;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.service.DomainService;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
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
import java.util.concurrent.atomic.AtomicBoolean;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.util.concurrent.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
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
    private final MatchesCreationFinalizer matchesCreationFinalizer;

    private final ExecutorService batchExecutor;
    private final ExecutorService semaphoreExecutor;

    private final Semaphore domainSemaphore;
    private final ConcurrentMap<UUID, CompletableFuture<Void>> groupLocks = new ConcurrentHashMap<>();

    @Autowired
    @Lazy
    private PotentialMatchesCreationScheduler self;

    @Value("${match.max-final-batch-size:50000}")
    private int maxFinalBatchSize;

    @Value("${match.max-concurrent-domains:2}")
    private int maxConcurrentDomains;

    private static final long CYCLE_TIMEOUT_HOURS = 20;
    private static final long SEMAPHORE_TIMEOUT_MINUTES = 120;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public PotentialMatchesCreationScheduler(
            DomainService domainService,
            MatchingGroupRepository matchingGroupRepository,
            PotentialMatchesCreationJobExecutor jobExecutor,
            PotentialMatchComputationProcessor processor,
            MeterRegistry meterRegistry,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            MatchesCreationFinalizer matchesCreationFinalizer,
            @Qualifier("semaphoreExecutor") ExecutorService semaphoreExecutor) {

        this.domainService = domainService;
        this.matchingGroupRepository = matchingGroupRepository;
        this.jobExecutor = jobExecutor;
        this.processor = processor;
        this.meterRegistry = meterRegistry;
        this.batchExecutor = batchExecutor;
        this.semaphoreExecutor = semaphoreExecutor;
        this.matchesCreationFinalizer = matchesCreationFinalizer;
        this.domainSemaphore = new Semaphore(2, true);
    }

    @PostConstruct
    public void init() {
        validateExecutorPool(batchExecutor, maxConcurrentDomains);
    }

    private void validateExecutorPool(ExecutorService executor, int concurrentDomains) {
        if (executor instanceof ThreadPoolExecutor tpe) {
            int poolSize = tpe.getMaximumPoolSize();
            int requiredMinimum = concurrentDomains * 2 + 5;

            if (poolSize < requiredMinimum) {
                log.warn("BatchExecutor pool size ({}) is critically small for maxConcurrentDomains ({}). Required minimum recommended: {}. Potential deadlock risk.",
                        poolSize, concurrentDomains, requiredMinimum);
            } else {
                log.info("BatchExecutor pool size ({}) is adequate for maxConcurrentDomains ({}).", poolSize, concurrentDomains);
            }
        } else {
            log.info("BatchExecutor is not a ThreadPoolExecutor (Type: {}). Cannot validate pool size.", executor.getClass().getSimpleName());
        }
    }

    private void cleanupIdleGroupLocks() {
        groupLocks.entrySet().removeIf(entry -> entry.getValue().isDone());
    }

    @Scheduled(cron = "0 38 20 * * *", zone = "Asia/Kolkata")
    public void processAllDomains() {
        if (!isSafeToRun()) {
            log.warn("Scheduler already running — skipping this invocation.");
            return;
        }

        try {
            execute();
        } catch (Exception e) {
            log.error("Unhandled exception during batch execution setup.", e);
        } finally {
            running.set(false);
        }
    }

    private void execute() {
        Timer.Sample sample = Timer.start(meterRegistry);
        String cycleId = DefaultValuesPopulator.getUid();
        Instant startTime = Instant.now();
        log.info("Starting BATCH MATCHING CYCLE | cycleId={} | startTime={}", cycleId, startTime);

        List<Domain> domains = domainService.getActiveDomains();
        if (domains.isEmpty()) {
            log.info("No active domains found. Skipping cycle.");
            sample.stop(meterRegistry.timer("batch_matches_total_duration"));
            return;
        }

        List<GroupTaskRequest> tasks = new ArrayList<>();
        for (Domain domain : domains) {
            List<UUID> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            groupIds.forEach(groupId -> tasks.add(new GroupTaskRequest(domain, groupId, cycleId)));
        }

        log.info("Found {} groups to process across {} domains. Max concurrent jobs: {}.",
                tasks.size(), domains.size(), maxConcurrentDomains);

        List<CompletableFuture<Void>> allTasks = tasks.stream()
                .map(req -> self.processGroupTask(req))
                .toList();

        CompletableFuture.allOf(allTasks.toArray(new CompletableFuture[0]))
                .orTimeout(CYCLE_TIMEOUT_HOURS, TimeUnit.HOURS)
                .whenCompleteAsync((v, error) -> {
                    handleCycleCompletion(cycleId, allTasks, error);
                    sample.stop(meterRegistry.timer("batch_matches_total_duration"));
                    cleanupIdleGroupLocks();
                }, batchExecutor);
    }

    private boolean isSafeToRun() {
        return running.compareAndSet(false, true);
    }

    @CircuitBreaker(name = "potentialMatchesGroup", fallbackMethod = "processGroupTaskFallback")
    @Retry(name = "potentialMatchesGroup", fallbackMethod = "processGroupTaskFallback")
    public CompletableFuture<Void> processGroupTask(GroupTaskRequest request) {
        return groupLocks.compute(request.groupId(), (k, previousTask) -> {
            if (previousTask == null || previousTask.isDone()) {
                log.debug("Executing new task chain for group {}", request.groupId());
                return executeGroupTaskChain(request);
            } else {
                log.info("Group {} is busy (Previous task not done). Chaining task.", request.groupId());
                return previousTask
                        .orTimeout(CYCLE_TIMEOUT_HOURS, TimeUnit.HOURS)
                        .handle((res, ex) -> {
                            if (ex != null) log.warn("Previous task for group {} timed out or failed in chain.", request.groupId());
                            return null;
                        })
                        .thenCompose(v -> executeGroupTaskChain(request));
            }
        });
    }

    public CompletableFuture<Void> processGroupTaskFallback(GroupTaskRequest request, Throwable t) {
        log.error("FALLBACK triggered for groupId={} domainId={} | cause: {}",
                request.groupId(), request.domain().getId(), t.toString(), t);

        meterRegistry.counter("potential_matches_fallback_total",
                "groupId", request.groupId().toString(),
                "domainId", request.domain().getId().toString(),
                "cause", t.getClass().getSimpleName()).increment();

        try {
            processor.cleanup(request.groupId());
        } catch (Exception ex) {
            log.error("Cleanup failed in fallback for groupId={}", request.groupId(), ex);
        }

        groupLocks.remove(request.groupId());

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> executeGroupTaskChain(GroupTaskRequest req) {
        AtomicBoolean permitAcquired = new AtomicBoolean(false);

        return acquireSemaphoreAsync(domainSemaphore, req.groupId())
                .thenCompose(acquired -> {
                    if (!acquired) {
                        log.error("Could not acquire permit for group {} — skipping processing", req.groupId());
                        meterRegistry.counter("semaphore_acquisition_failures", "groupId", req.groupId().toString()).increment();
                        return CompletableFuture.completedFuture(null);
                    }
                    permitAcquired.set(true);
                    log.info("PERMIT ACQUIRED. START PROCESSING | groupId={} | domainId={}", req.groupId(), req.domain().getId());

                    return CompletableFuture.supplyAsync(() -> null, batchExecutor)
                            .thenCompose(v -> jobExecutor.processGroup(req.groupId(), req.domain().getId(), req.cycleId()))
                            .thenCompose(v -> processor.savePendingMatchesAsync(req.groupId(), req.domain().getId(), req.cycleId(), maxFinalBatchSize))
                            .thenCompose(v -> processor.saveFinalMatches(req.groupId(), req.domain().getId(), req.cycleId(), null, -1));
                })
                .whenCompleteAsync((v, error) -> {
                    if (permitAcquired.get()) {
                        try {
                            domainSemaphore.release();
                            log.info("PERMIT RELEASED | groupId={}", req.groupId());
                        } catch (Exception ex) {
                            log.error("Failed to release semaphore for groupId={}", req.groupId(), ex);
                        }
                    }

                    try {
                        processor.cleanup(req.groupId());
                    } catch (Exception ex) {
                        log.error("Cleanup failed after completion for groupId={}", req.groupId(), ex);
                    }

                    if (error != null) {
                        log.error("FAILED processing groupId={} | error={}", req.groupId(), error.getMessage(), error);
                        meterRegistry.counter("matches_creation_error", "groupId", req.groupId().toString()).increment();
                    } else {
                        log.info("SUCCESS processing groupId={}", req.groupId());
                    }

                    matchesCreationFinalizer.finalize(false);
                }, batchExecutor);
    }

    private void handleCycleCompletion(String cycleId, List<CompletableFuture<Void>> tasks, Throwable error) {
        if (error instanceof TimeoutException) {
            log.error("BATCH CYCLE TIMEOUT ({}h) | cycleId={}", CYCLE_TIMEOUT_HOURS, cycleId);
            tasks.forEach(f -> f.cancel(true));
            meterRegistry.counter("batch_cycle_timeout").increment();
        } else if (error != null) {
            log.error("BATCH CYCLE FAILED | cycleId={}", cycleId, error);
            meterRegistry.counter("batch_cycle_failure").increment();
        } else {
            log.info("BATCH CYCLE COMPLETED SUCCESSFULLY | cycleId={}", cycleId);
        }

        matchesCreationFinalizer.finalize(true);
    }

    private CompletableFuture<Boolean> acquireSemaphoreAsync(Semaphore semaphore, UUID debugId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("Attempting to acquire semaphore for {}", debugId);
                boolean acquired = semaphore.tryAcquire(SEMAPHORE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
                if (!acquired) {
                    log.warn("Semaphore acquisition timed out for {}", debugId);
                }
                return acquired;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
        }, semaphoreExecutor);
    }
}