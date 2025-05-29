package com.shedule.x.scheduler;

import com.shedule.x.config.QueueManagerConfig;
import com.shedule.x.config.factory.QueueManagerFactory;
import com.shedule.x.models.Domain;
import com.shedule.x.processors.MatchesCreationJobExecutor;
import com.shedule.x.processors.QueueManagerImpl;
import com.shedule.x.repo.DomainRepository;
import com.shedule.x.repo.MatchingGroupRepository;
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


@Slf4j
@Component
@Profile("!singleton")
public class MatchesCreationScheduler {
    private final DomainRepository domainRepository;
    private final MatchingGroupRepository matchingGroupRepository;
    private final MatchesCreationJobExecutor jobExecutor;
    private final MeterRegistry meterRegistry;
    private final Semaphore domainSemaphore;
    private final ConcurrentMap<String, Semaphore> groupLocks = new ConcurrentHashMap<>();
    private final ExecutorService batchExecutor;
    private final QueueManagerFactory queueManagerFactory;

    @Value("${match.queue.capacity:9000000}")
    private int queueCapacity;

    @Value("${match.flush-interval-seconds:5}")
    private int flushIntervalSeconds;

    @Value("${match.drain-warning-threshold:0.9}")
    private double drainWarningThreshold;

    @Value("${match.boost-batch-factor:2}")
    private int boostBatchFactor;

    @Value("${match.max-final-batch-size:50000}")
    private int maxFinalBatchSize;

    @Value("${match.shutdown-limit-seconds:1800}")
    private int shutdownLimitSeconds;

    @Value("${match.max-iterations:1000}")
    private int maxIterations;

    public MatchesCreationScheduler(
            DomainRepository domainRepository,
            MatchingGroupRepository matchingGroupRepository,
            MatchesCreationJobExecutor jobExecutor,
            MeterRegistry meterRegistry,
            QueueManagerFactory queueManagerFactory,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            @Value("${match.max-concurrent-domains:2}") int maxConcurrentDomains
    ) {
        this.domainRepository = domainRepository;
        this.matchingGroupRepository = matchingGroupRepository;
        this.jobExecutor = jobExecutor;
        this.meterRegistry = meterRegistry;
        this.queueManagerFactory = queueManagerFactory;
        this.domainSemaphore = new Semaphore(maxConcurrentDomains, true);
        this.batchExecutor = batchExecutor;
        if (batchExecutor instanceof ThreadPoolExecutor tpe) {
            int poolSize = tpe.getMaximumPoolSize();
            int requiredSize = maxConcurrentDomains + 2;
            if (poolSize < requiredSize) {
                throw new IllegalArgumentException(
                        "batchExecutor max pool size (" + poolSize + ") must be >= " + requiredSize);
            }
        }
    }

    @Scheduled(fixedDelayString = "${match.save.delay:900000}")
    public void processAllDomainsScheduled() {
        Timer.Sample sample = Timer.start(meterRegistry);
        String cycleId = DefaultValuesPopulator.getUid();
        log.info("Starting batch matching at {}, cycleId={}", DefaultValuesPopulator.getCurrentTimestamp(), cycleId);
        logExecutorState();

        List<Domain> domains = domainRepository.findByIsActiveTrue();
        if (domains.isEmpty()) {
            log.info("No active domains to process");
            sample.stop(meterRegistry.timer("batch_matches_total_duration"));
            return;
        }

        List<CompletableFuture<Void>> batches = new ArrayList<>();
        List<Map.Entry<Domain, String>> tasks = new ArrayList<>();
        for (Domain domain : domains) {
            List<String> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            for (String groupId : groupIds) {
                tasks.add(new AbstractMap.SimpleEntry<>(domain, groupId));
            }
        }

        for (Map.Entry<Domain, String> task : tasks) {
            Domain domain = task.getKey();
            String groupId = task.getValue();
            batches.add(processGroupTask(groupId, domain.getId(), cycleId));
        }

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(batches.toArray(new CompletableFuture[0]))
                .thenAcceptAsync(v -> {
                    log.info("Completed batch matching at {}, cycleId={}", DefaultValuesPopulator.getCurrentTimestamp(), cycleId);
                    resultFuture.complete(null);
                }, batchExecutor)
                .exceptionally(t -> {
                    log.error("Batch processing failed, cycleId={}: {}", cycleId, t.getMessage(), t);
                    resultFuture.completeExceptionally(t);
                    return null;
                });

        try {
            resultFuture.get(60, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("Error waiting for batch completion, cycleId={}: {}", cycleId, e.getMessage(), e);
        }

        groupLocks.entrySet().removeIf(entry -> entry.getValue().availablePermits() == 1 && !entry.getValue().hasQueuedThreads());
        logExecutorState();
        sample.stop(meterRegistry.timer("batch_matches_total_duration"));
    }

    private CompletableFuture<Void> processGroupTask(String groupId, UUID domainId, String cycleId) {
        Semaphore groupSemaphore = groupLocks.computeIfAbsent(groupId, id -> new Semaphore(1, true));
        Timer.Sample batchTimer = Timer.start(meterRegistry);

        return CompletableFuture.runAsync(() -> {
                    try {
                        boolean acquired = domainSemaphore.tryAcquire(30, TimeUnit.SECONDS);
                        if (!acquired) {
                            throw new TimeoutException("Timed out acquiring domainSemaphore for domainId=" + domainId);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted acquiring domainSemaphore", e);
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }, batchExecutor)
                .thenCompose(v -> CompletableFuture.runAsync(() -> {
                    try {
                        boolean acquired = groupSemaphore.tryAcquire(30, TimeUnit.SECONDS);
                        if (!acquired) {
                            throw new TimeoutException("Timed out acquiring groupSemaphore for groupId=" + groupId);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted acquiring groupSemaphore", e);
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }, batchExecutor))
                .thenCompose(v -> {
                    log.info("Acquired locks for groupId={} domainId={}, cycleId={}", groupId, domainId, cycleId);
                    logSemaphoreState(groupId);
                    return jobExecutor.processGroup(groupId, domainId, cycleId)
                            .thenRunAsync(() -> doFlushLoop(groupId, domainId, cycleId), batchExecutor);
                })
                .whenComplete((v, e) -> {
                    groupSemaphore.release();
                    domainSemaphore.release();
                    log.info("Released locks for groupId={} domainId={} cycleId={}, permits: domain={} group={}",
                            groupId, domainId, cycleId,
                            domainSemaphore.availablePermits(), groupSemaphore.availablePermits());

                    if (e != null) {
                        meterRegistry.counter("matches_creation_error", "mode", "batch",
                                "domainId", domainId.toString(), "groupId", groupId, "cycleId", cycleId).increment();
                        log.error("Error in batch+flush for groupId={} domainId={} cycleId={}: {}",
                                groupId, domainId, cycleId, e.getMessage(), e);
                    }

                    batchTimer.stop(meterRegistry.timer("batch_matches_duration",
                            "domainId", domainId.toString(), "groupId", groupId, "cycleId", cycleId));
                });
    }

    private void doFlushLoop(String groupId, UUID domainId, String cycleId) {
        QueueManagerConfig config = new QueueManagerConfig(
                queueCapacity, flushIntervalSeconds, drainWarningThreshold, boostBatchFactor, maxFinalBatchSize
        );
        QueueManagerImpl queueManager = queueManagerFactory.create(groupId, domainId, DefaultValuesPopulator.getUid(), config);
        if (queueManager == null) {
            log.warn("No QueueManager for groupId={} domainId={} cycleId={}", groupId, domainId, cycleId);
            return;
        }

        long queueSize = 0;
        long startTime = System.currentTimeMillis();
        CompletableFuture<Void> flushFuture = CompletableFuture.completedFuture(null);
        for (int i = 0; i < maxIterations; i++) {
            queueSize = queueManager.getQueueSize();
            log.info("Iteration {}: queueSize={} for groupId={} cycleId={}", i, queueSize, groupId, cycleId);
            if (queueSize == 0) break;

            flushFuture = flushFuture.thenComposeAsync(v -> {
                QueueManagerImpl.flushQueueBlocking(groupId, queueManagerFactory.getFlushSignalCallback());
                long newQueueSize = queueManager.getQueueSize();
                if (newQueueSize > 0) {
                    log.info("Remaining {} queued matches for groupId={} cycleId={}", newQueueSize, groupId, cycleId);
                    return CompletableFuture.runAsync(() -> {}, CompletableFuture.delayedExecutor(300, TimeUnit.MILLISECONDS, batchExecutor));
                }
                return CompletableFuture.completedFuture(null);
            }, batchExecutor);

            if (System.currentTimeMillis() - startTime > shutdownLimitSeconds * 1000) {
                log.warn("Shutdown limit of {}s reached, {} matches remain for groupId={} cycleId={}",
                        shutdownLimitSeconds, queueSize, groupId, cycleId);
                meterRegistry.counter("matches_dropped_due_to_shutdown", "groupId", groupId,
                        "domainId", domainId.toString(), "cycleId", cycleId).increment(queueSize);
                break;
            }
        }

        try {
            flushFuture.get(shutdownLimitSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("Flush timeout for groupId={} cycleId={}", groupId, cycleId);
        } catch (Exception e) {
            log.error("Flush failed for groupId={} cycleId={}: {}", groupId, cycleId, e.getMessage());
        }

        queueSize = queueManager.getQueueSize();
        if (queueSize > 0) {
            log.warn("Max iterations {} reached, {} matches remain for groupId={} cycleId={}",
                    maxIterations, queueSize, groupId, cycleId);
            meterRegistry.counter("matches_dropped_due_to_max_iterations", "groupId", groupId,
                    "domainId", domainId.toString(), "cycleId", cycleId).increment(queueSize);
        }
        log.info("Completed flush for groupId={} cycleId={}", groupId, cycleId);
    }

    private void logExecutorState() {
        if (batchExecutor instanceof ThreadPoolExecutor tpe) {
            log.info("Async pool: active={} queue={} completed={}",
                    tpe.getActiveCount(), tpe.getQueue().size(), tpe.getCompletedTaskCount());
        }
    }

    private void logSemaphoreState(String groupId) {
        log.info("Permits: domain={} group={}",
                domainSemaphore.availablePermits(),
                groupLocks.getOrDefault(groupId, new Semaphore(1)).availablePermits());
    }
}