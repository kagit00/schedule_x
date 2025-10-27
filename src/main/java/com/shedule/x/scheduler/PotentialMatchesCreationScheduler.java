package com.shedule.x.scheduler;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.config.QueueManagerConfig;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.config.factory.QueueManagerFactory;
import com.shedule.x.models.Domain;
import com.shedule.x.processors.MatchesCreationFinalizer;
import com.shedule.x.processors.PotentialMatchesCreationJobExecutor;
import com.shedule.x.processors.QueueManagerImpl;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.*;
import java.util.concurrent.*;


@Slf4j
@Component
@Profile("!singleton")
public class PotentialMatchesCreationScheduler {
    private final DomainService domainService;
    private final MatchingGroupRepository matchingGroupRepository;
    private final PotentialMatchesCreationJobExecutor jobExecutor;
    private final MeterRegistry meterRegistry;
    private final Semaphore domainSemaphore;
    private final ConcurrentMap<UUID, Semaphore> groupLocks = new ConcurrentHashMap<>();
    private final ExecutorService batchExecutor;
    private final QueueManagerFactory queueManagerFactory;
    private final MatchesCreationFinalizer matchesCreationFinalizer;

    @Value("${match.queue.capacity:500000}")
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

    public PotentialMatchesCreationScheduler(
            DomainService domainService,
            MatchingGroupRepository matchingGroupRepository,
            PotentialMatchesCreationJobExecutor jobExecutor,
            MeterRegistry meterRegistry,
            QueueManagerFactory queueManagerFactory,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            @Value("${match.max-concurrent-domains:2}") int maxConcurrentDomains,
            MatchesCreationFinalizer matchesCreationFinalizer
    ) {
        this.domainService = domainService;
        this.matchingGroupRepository = matchingGroupRepository;
        this.jobExecutor = jobExecutor;
        this.meterRegistry = meterRegistry;
        this.queueManagerFactory = queueManagerFactory;
        this.domainSemaphore = new Semaphore(maxConcurrentDomains, true);
        this.batchExecutor = batchExecutor;
        this.matchesCreationFinalizer = matchesCreationFinalizer;

        if (batchExecutor instanceof ThreadPoolExecutor tpe) {
            int poolSize = tpe.getMaximumPoolSize();
            int requiredSize = maxConcurrentDomains + 2;
            if (poolSize < requiredSize) {
                throw new IllegalArgumentException(
                        "batchExecutor max pool size (" + poolSize + ") must be >= " + requiredSize);
            }
        }
    }

    @Scheduled(fixedDelayString = "${match.save.delay:600000}")
    public void processAllDomainsScheduled() {
        Timer.Sample sample = Timer.start(meterRegistry);
        String cycleId = DefaultValuesPopulator.getUid();
        log.info("Starting batch matching at {}, cycleId={}", DefaultValuesPopulator.getCurrentTimestamp(), cycleId);

        List<Domain> domains = domainService.getActiveDomains();
        if (domains.isEmpty()) {
            log.info("No active domains to process");
            sample.stop(meterRegistry.timer("batch_matches_total_duration"));
            return;
        }

        List<CompletableFuture<Void>> batches = new ArrayList<>();
        List<Map.Entry<Domain, UUID>> tasks = new ArrayList<>();
        for (Domain domain : domains) {
            List<UUID> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            for (UUID groupId : groupIds) {
                tasks.add(new AbstractMap.SimpleEntry<>(domain, groupId));
            }
        }

        for (Map.Entry<Domain, UUID> task : tasks) {
            Domain domain = task.getKey();
            UUID groupId = task.getValue();
            batches.add(processGroupTask(groupId, domain.getId(), cycleId));
        }

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        CompletableFuture.allOf(batches.toArray(new CompletableFuture[0]))
                .thenAcceptAsync(v -> {
                    log.info("Completed batch matching at {}, cycleId={}", DefaultValuesPopulator.getCurrentTimestamp(), cycleId);
                    resultFuture.complete(null);
                    tasks.forEach(task -> {
                        matchesCreationFinalizer.finalize(true);
                    });
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
        sample.stop(meterRegistry.timer("batch_matches_total_duration"));
    }

    private CompletableFuture<Void> processGroupTask(UUID groupId, UUID domainId, String cycleId) {
        Semaphore groupSemaphore = groupLocks.computeIfAbsent(groupId, id -> new Semaphore(1, true));
        Timer.Sample batchTimer = Timer.start(meterRegistry);

        AtomicBoolean domainAcquired = new AtomicBoolean(false);
        AtomicBoolean groupAcquired = new AtomicBoolean(false);

        return CompletableFuture.runAsync(() -> {
                    try {
                        boolean acquired = domainSemaphore.tryAcquire(30, TimeUnit.SECONDS);
                        if (!acquired) {
                            throw new TimeoutException("Timed out acquiring domainSemaphore for domainId=" + domainId);
                        }
                        domainAcquired.set(true);
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
                        groupAcquired.set(true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted acquiring groupSemaphore", e);
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                }, batchExecutor))
                .thenCompose(v -> {
                    log.info("Acquired locks for groupId={} domainId={}, cycleId={}", groupId, domainId, cycleId);
                    return jobExecutor.processGroup(groupId, domainId, cycleId)
                            .thenRunAsync(() -> doFlushLoop(groupId, domainId, cycleId), batchExecutor);
                })
                .whenComplete((v, e) -> {
                    if (groupAcquired.get()) {
                        try {
                            groupSemaphore.release();
                        } catch (Exception ex) {
                            log.error("Failed to release groupSemaphore for groupId={}: {}", groupId, ex.getMessage(), ex);
                        }
                    } else {
                        log.debug("Group semaphore was not acquired for groupId={}, skipping release", groupId);
                    }

                    if (domainAcquired.get()) {
                        try {
                            domainSemaphore.release();
                        } catch (Exception ex) {
                            log.error("Failed to release domainSemaphore for domainId={}: {}", domainId, ex.getMessage(), ex);
                        }
                    } else {
                        log.debug("Domain semaphore was not acquired for domainId={}, skipping release", domainId);
                    }

                    try {
                        matchesCreationFinalizer.finalize(false);
                    } catch (Exception ex) {
                        log.warn("matchesCreationFinalizer.finalize(false) threw for groupId={}, cycleId={}: {}", groupId, cycleId, ex.getMessage());
                    }

                    log.info("Released locks for groupId={} domainId={} cycleId={}, permits: domain={} group={}",
                            groupId, domainId, cycleId,
                            domainSemaphore.availablePermits(), groupSemaphore.availablePermits());

                    if (e != null) {
                        meterRegistry.counter("matches_creation_error", "mode", "batch",
                                "domainId", domainId.toString(), "groupId", groupId.toString(), "cycleId", cycleId).increment();
                        log.error("Error in batch+flush for groupId={} domainId={} cycleId={}: {}",
                                groupId, domainId, cycleId, e.getMessage(), e);
                    }

                    if (groupSemaphore.availablePermits() == 1 && !groupSemaphore.hasQueuedThreads()) {
                        groupLocks.remove(groupId, groupSemaphore);
                    }

                    batchTimer.stop(meterRegistry.timer("batch_matches_duration",
                            "domainId", domainId.toString(), "groupId", groupId.toString(), "cycleId", cycleId));
                });
    }

    private void doFlushLoop(UUID groupId, UUID domainId, String cycleId) {
        QueueManagerConfig config = new QueueManagerConfig(
                queueCapacity, flushIntervalSeconds, drainWarningThreshold, boostBatchFactor, maxFinalBatchSize
        );

        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, DefaultValuesPopulator.getUid(), config);
        QueueManagerImpl queueManager = queueManagerFactory.create(queueConfig);

        if (Objects.isNull(queueManager)) {
            log.warn("No QueueManager for groupId={} domainId={} cycleId={}", groupId, domainId, cycleId);
            return;
        }

        long queueSize;
        long startTime = System.currentTimeMillis();
        CompletableFuture<Void> flushFuture = CompletableFuture.completedFuture(null);
        for (int i = 0; i < maxIterations; i++) {
            queueSize = queueManager.getQueueSize();
            log.debug("Iteration {}: queueSize={} for groupId={} cycleId={}", i, queueSize, groupId, cycleId);
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

            if (System.currentTimeMillis() - startTime > shutdownLimitSeconds * 1000L) {
                log.warn("Shutdown limit of {}s reached, {} matches remain for groupId={} cycleId={}",
                        shutdownLimitSeconds, queueSize, groupId, cycleId);
                meterRegistry.counter("matches_dropped_due_to_shutdown", "groupId", groupId.toString(),
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
            meterRegistry.counter("matches_dropped_due_to_max_iterations", "groupId", groupId.toString(),
                    "domainId", domainId.toString(), "cycleId", cycleId).increment(queueSize);
        }
        log.info("Completed flush for groupId={} cycleId={}", groupId, cycleId);
    }
}
