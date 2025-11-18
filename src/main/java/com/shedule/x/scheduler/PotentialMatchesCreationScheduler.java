package com.shedule.x.scheduler;

import com.google.common.util.concurrent.Uninterruptibles;
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

    @Value("${match.shutdown-limit-seconds:3600}")
    private int shutdownLimitSeconds;

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
            int requiredSize = maxConcurrentDomains + 4;
            if (poolSize < requiredSize) {
                throw new IllegalArgumentException(
                        "batchExecutor max pool size (" + poolSize + ") must be >= " + requiredSize);
            }
        }
    }

    @Scheduled(fixedDelayString = "${match.save.delay:300000}")
    public void processAllDomainsScheduled() {
        Timer.Sample sample = Timer.start(meterRegistry);
        String cycleId = DefaultValuesPopulator.getUid();
        log.info("Starting batch matching cycleId={}", cycleId);

        List<Domain> domains = domainService.getActiveDomains();
        if (domains.isEmpty()) {
            log.info("No active domains");
            sample.stop(meterRegistry.timer("batch_matches_total_duration"));
            return;
        }

        List<CompletableFuture<Void>> allTasks = new ArrayList<>();
        List<Map.Entry<Domain, UUID>> taskList = new ArrayList<>();

        for (Domain domain : domains) {
            List<UUID> groupIds = matchingGroupRepository.findGroupIdsByDomainId(domain.getId());
            for (UUID groupId : groupIds) {
                taskList.add(new AbstractMap.SimpleEntry<>(domain, groupId));
            }
        }

        for (Map.Entry<Domain, UUID> task : taskList) {
            UUID domainId = task.getKey().getId();
            UUID groupId = task.getValue();
            allTasks.add(processGroupTask(groupId, domainId, cycleId));
        }

        CompletableFuture.runAsync(() -> {
            try {
                CompletableFuture.allOf(allTasks.toArray(new CompletableFuture[0]))
                        .get(3, TimeUnit.HOURS);

                log.info("BATCH CYCLE COMPLETED SUCCESSFULLY | cycleId={} | groups={}", cycleId, allTasks.size());
                taskList.forEach(t -> matchesCreationFinalizer.finalize(true));

            } catch (TimeoutException te) {
                log.error("BATCH CYCLE TIMEOUT AFTER 3 HOURS | cycleId={}. Killing all remaining tasks...", cycleId, te);
                allTasks.forEach(f -> f.cancel(true));
                meterRegistry.counter("batch_cycle_timeout_total").increment();

            } catch (Exception e) {
                log.error("BATCH CYCLE FAILED | cycleId={}", cycleId, e);
                meterRegistry.counter("batch_cycle_failure_total").increment();

            } finally {
                sample.stop(meterRegistry.timer("batch_matches_total_duration"));
                cleanupIdleGroupLocks();
            }
        }, batchExecutor);
    }

    private void cleanupIdleGroupLocks() {
        groupLocks.entrySet().removeIf(entry -> {
            Semaphore s = entry.getValue();
            return s.availablePermits() == 1 && !s.hasQueuedThreads();
        });
    }

    private CompletableFuture<Void> processGroupTask(UUID groupId, UUID domainId, String cycleId) {
        Semaphore groupSemaphore = groupLocks.computeIfAbsent(groupId, k -> new Semaphore(1, true));

        return CompletableFuture.runAsync(() -> {
                    domainSemaphore.acquireUninterruptibly();
                    try {
                        groupSemaphore.acquireUninterruptibly();
                        try {
                            log.info("START groupId={} domainId={} cycleId={}", groupId, domainId, cycleId);

                            jobExecutor.processGroup(groupId, domainId, cycleId)
                                    .thenRunAsync(() -> doFlushLoop(groupId, domainId, cycleId), batchExecutor)
                                    .join();

                            log.info("SUCCESS groupId={} domainId={} cycleId={}", groupId, domainId, cycleId);
                        } finally {
                            groupSemaphore.release();
                        }
                    } finally {
                        domainSemaphore.release();
                    }
                }, batchExecutor)
                .whenComplete((v, t) -> {
                    if (t != null) {
                        log.error("FAILED groupId={} domainId={} cycleId={}", groupId, domainId, cycleId, t);
                        meterRegistry.counter("matches_creation_error", "groupId", groupId.toString()).increment();
                    }
                    if (groupSemaphore.availablePermits() == 1 && !groupSemaphore.hasQueuedThreads()) {
                        groupLocks.remove(groupId, groupSemaphore);
                    }
                    matchesCreationFinalizer.finalize(false);
                });
    }

    private void doFlushLoop(UUID groupId, UUID domainId, String cycleId) {
        QueueManagerConfig config = new QueueManagerConfig(
                queueCapacity, flushIntervalSeconds, drainWarningThreshold, boostBatchFactor, maxFinalBatchSize);
        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, DefaultValuesPopulator.getUid(), config);
        QueueManagerImpl queueManager = queueManagerFactory.create(queueConfig);

        if (queueManager == null) {
            log.warn("No QueueManager created for groupId={}", groupId);
            return;
        }

        long deadline = System.currentTimeMillis() + (shutdownLimitSeconds * 1000L);

        while (true) {
            long queueSize = queueManager.getQueueSize();
            if (queueSize == 0) {
                log.info("Flush complete: all queued matches processed for groupId={}", groupId);
                break;
            }

            if (System.currentTimeMillis() > deadline) {
                log.error("FLUSH DEADLINE EXCEEDED ({}s). Dropping {} queued matches for groupId={}",
                        shutdownLimitSeconds, queueSize, groupId);
                meterRegistry.counter("matches_dropped_due_to_flush_deadline", "groupId", groupId.toString()).increment(queueSize);
                break;
            }

            QueueManagerImpl.flushQueueBlocking(groupId, queueManagerFactory.getFlushSignalCallback());

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
    }
}