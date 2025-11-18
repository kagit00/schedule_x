package com.shedule.x.processors;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.basic.FlushUtils;
import com.shedule.x.utils.basic.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
public class QueueManagerImpl {
    private static final ConcurrentHashMap<UUID, WeakReference<QueueManagerImpl>> INSTANCES = new ConcurrentHashMap<>();
    private static final int REDUCED_MAX_FINAL_BATCH_SIZE = 10_000;
    private static final long TTL_SECONDS = 86_400;
    private static final int DEBOUNCE_WINDOW_MS = 1_000;

    private final BlockingQueue<GraphRecords.PotentialMatch> queue;
    private final QueueConfig config;
    private final AtomicLong lastFlushedQueueSize = new AtomicLong(0);
    private final AtomicLong enqueueCount = new AtomicLong(0);
    private final AtomicLong dequeueCount = new AtomicLong(0);
    private final ScheduledExecutorService flushScheduler;
    private final ExecutorService mappingExecutor;
    private final ExecutorService flushExecutor;
    private final QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger flushIntervalSeconds;
    private volatile ScheduledFuture<?> flushTask;
    private volatile long lastFlushIntervalSetAt = 0;
    private final AtomicBoolean boostedDrainInProgress = new AtomicBoolean(false);
    private final long creationTime;
    private final Semaphore periodicFlushSemaphore;
    private final Semaphore blockingFlushSemaphore;
    private final Semaphore boostedFlushSemaphore;
    private final ReentrantLock metricsLock = new ReentrantLock();
    private final ReentrantLock scheduleLock = new ReentrantLock();
    private final AtomicLong scheduleGeneration = new AtomicLong(0);

    @FunctionalInterface
    public interface QuadFunction<T, U, V, W, R> {
        R apply(T t, U u, V v, W w);
    }

    private QueueManagerImpl(QueueConfig config, MeterRegistry meterRegistry, ExecutorService mappingExecutor,
                             ExecutorService flushExecutor, ScheduledExecutorService flushScheduler,
                             QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback) {
        this.config = config;
        this.queue = new LinkedBlockingQueue<>(config.getCapacity());
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.mappingExecutor = Objects.requireNonNull(mappingExecutor, "mappingExecutor must not be null");
        this.flushExecutor = Objects.requireNonNull(flushExecutor, "flushExecutor must not be null");
        this.flushScheduler = Objects.requireNonNull(flushScheduler, "flushScheduler must not be null");
        this.flushSignalCallback = Objects.requireNonNull(flushSignalCallback, "flushSignalCallback must not be null");
        this.flushIntervalSeconds = new AtomicInteger(config.getFlushIntervalSeconds());
        this.creationTime = System.currentTimeMillis();
        this.periodicFlushSemaphore = new Semaphore(2, true);
        this.blockingFlushSemaphore = new Semaphore(2, true);
        this.boostedFlushSemaphore = new Semaphore(1, true);

        MetricsUtils.registerQueueMetrics(meterRegistry, config.getGroupId(), queue, config.getCapacity());
        startPeriodicFlush();
        scheduleInstanceTTLEviction();
        Runtime.getRuntime().addShutdownHook(new Thread(QueueManagerImpl::removeAll, "QueueManagerShutdown"));
        log.info("Initialized QueueManager for groupId={}, domainId={}, processingCycleId={}",
                config.getGroupId(), config.getDomainId(), config.getProcessingCycleId());
    }

    public long getQueueSize() {
        return queue.size();
    }

    public boolean enqueue(GraphRecords.PotentialMatch match) {
        try {
            if (!queue.offer(match, 1, TimeUnit.SECONDS)) {
                log.warn("Failed to enqueue match for groupId={} due to full queue", config.getGroupId());
                return false;
            }
            enqueueCount.incrementAndGet();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted enqueuing match for groupId={}", config.getGroupId(), e);
            return false;
        }
    }

    public static void flushQueueBlocking(UUID groupId, QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushCallback) {
        WeakReference<QueueManagerImpl> ref = INSTANCES.get(groupId);
        QueueManagerImpl manager = ref != null ? ref.get() : null;
        if (manager == null || manager.getQueue().isEmpty()) {
            return;
        }
        FlushUtils.executeBlockingFlush(manager.blockingFlushSemaphore, flushCallback, groupId, manager.config.getDomainId(),
                Math.min(manager.getQueue().size(), manager.config.getMaxFinalBatchSize()), manager.config.getProcessingCycleId());
    }


    private void scheduleInstanceTTLEviction() {
        flushScheduler.schedule(() -> {
            long now = System.currentTimeMillis();
            synchronized (INSTANCES) {
                if (now - creationTime > TTL_SECONDS * 1000) {
                    log.info("Evicting QueueManager for groupId={} due to TTL expiration", config.getGroupId());
                    remove(config.getGroupId());
                }
            }
        }, TTL_SECONDS, TimeUnit.SECONDS);
    }

    public void checkQueueHealth() {
        double fillRatio = (double) queue.size() / config.getCapacity();
        if (fillRatio > config.getDrainWarningThreshold() && boostedDrainInProgress.compareAndSet(false, true)) {
            log.warn("Queue for groupId={} at {}% capacity (size={}/{}), triggering boosted drain",
                    config.getGroupId(), String.format("%.1f", fillRatio * 100), queue.size(), config.getCapacity());
            meterRegistry.counter("queue_drain_warnings_total", "groupId", config.getGroupId().toString()).increment();
            int boostedBatchSize = Math.min(queue.size(), REDUCED_MAX_FINAL_BATCH_SIZE * config.getBoostBatchFactor());
            FlushUtils.executeBoostedFlush(boostedFlushSemaphore, flushExecutor, flushSignalCallback,
                    config.getGroupId(), config.getDomainId(), boostedBatchSize, config.getProcessingCycleId(),
                    boostedDrainInProgress);
        }
        if (fillRatio > 0) {
            log.debug("Queue health for groupId={}: size={}/{}, fillRatio={}",
                    config.getGroupId(), queue.size(), config.getCapacity(), String.format("%.3f", fillRatio));
        }
    }

    public static void remove(UUID groupId) {
        synchronized (INSTANCES) {
            WeakReference<QueueManagerImpl> ref = INSTANCES.remove(groupId);
            if (ref != null && ref.get() != null) {
                log.info("Removed QueueManager for groupId={}", groupId);
                if (Objects.requireNonNull(ref.get()).flushTask != null) {
                    Objects.requireNonNull(ref.get()).flushTask.cancel(false);
                }
            }
        }
    }

    public static void removeAll() {
        synchronized (INSTANCES) {
            INSTANCES.keySet().forEach(QueueManagerImpl::remove);
        }
    }

    private void startPeriodicFlush() {
        long generation = scheduleGeneration.get();
        flushTask = flushScheduler.scheduleWithFixedDelay(() -> {
            if (scheduleGeneration.get() != generation) {
                return;
            }
            try {
                int queueSize = queue.size();
                long lastSize = lastFlushedQueueSize.get();

                if (queueSize > 0 && (queueSize >= lastSize * 1.1 || queueSize >= REDUCED_MAX_FINAL_BATCH_SIZE)) {
                    int batchSize = Math.min(queueSize, REDUCED_MAX_FINAL_BATCH_SIZE);

                    CompletableFuture.runAsync(() ->
                                    FlushUtils.executeFlush(periodicFlushSemaphore, flushExecutor, flushSignalCallback,
                                            config.getGroupId(), config.getDomainId(), batchSize, config.getProcessingCycleId(),
                                            meterRegistry, lastFlushedQueueSize),
                            flushExecutor);
                }

                checkQueueHealth();

            } catch (Exception e) {
                log.error("Error in periodic flush task for groupId={}", config.getGroupId(), e);
            }
        }, flushIntervalSeconds.get() / 2, flushIntervalSeconds.get(), TimeUnit.SECONDS);
    }

    public static CompletableFuture<Void> flushAllQueuesAsync(
            QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushCallback) {

        List<CompletableFuture<Void>> allFlushes = new ArrayList<>();

        INSTANCES.forEach((groupId, ref) -> {
            CompletableFuture<Void> flush = CompletableFuture.runAsync(() ->
                            flushQueueBlocking(groupId, flushCallback))
                    .orTimeout(30, TimeUnit.SECONDS)
                    .exceptionally(e -> {
                        log.error("Flush timeout or error for groupId={}", groupId, e);
                        return null;
                    });

            allFlushes.add(flush);
        });

        return CompletableFuture.allOf(allFlushes.toArray(new CompletableFuture[0]));
    }

    public void setFlushInterval(int newIntervalSeconds) {
        if (newIntervalSeconds <= 0) return;
        if (System.currentTimeMillis() - lastFlushIntervalSetAt < DEBOUNCE_WINDOW_MS) return;

        try {
            if (scheduleLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                try {
                    int oldInterval = flushIntervalSeconds.getAndSet(newIntervalSeconds);
                    lastFlushIntervalSetAt = System.currentTimeMillis();

                    if (flushTask != null) {
                        flushTask.cancel(false);
                    }

                    scheduleGeneration.incrementAndGet();

                    CompletableFuture.runAsync(this::startPeriodicFlush, flushScheduler);

                } finally {
                    scheduleLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    public static QueueManagerImpl getOrCreate(
            QueueConfig config,
            MeterRegistry meterRegistry,
            ExecutorService mappingExecutor,
            ExecutorService flushExecutor,
            ScheduledExecutorService flushScheduler,
            QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback
    ) {
        UUID groupId = config.getGroupId();

        WeakReference<QueueManagerImpl> ref = INSTANCES.get(groupId);
        QueueManagerImpl existing = ref != null ? ref.get() : null;

        if (existing != null) {
            if (!existing.config.getDomainId().equals(config.getDomainId())) {
                throw new IllegalStateException(
                        "GroupId " + groupId + " is already associated with domainId " +
                                existing.config.getDomainId() + ", cannot use domainId " + config.getDomainId()
                );
            }

            if (existing.config.getProcessingCycleId().equals(config.getProcessingCycleId())) {
                log.debug("Reusing existing QueueManager for groupId={}, processingCycleId={}",
                        groupId, config.getProcessingCycleId());
                return existing;
            } else {
                log.info("Cycle mismatch detected for groupId={}: {} -> {}. Will evict and recreate.",
                        groupId, existing.config.getProcessingCycleId(), config.getProcessingCycleId());

                synchronized (INSTANCES) {
                    WeakReference<QueueManagerImpl> currentRef = INSTANCES.get(groupId);
                    QueueManagerImpl current = currentRef != null ? currentRef.get() : null;

                    if (current == existing) {
                        INSTANCES.remove(groupId);
                        log.info("Evicted QueueManager for groupId={}", groupId);

                        QueueManagerImpl finalExisting = existing;
                        CompletableFuture.runAsync(() -> {
                            if (finalExisting.flushTask != null) {
                                finalExisting.flushTask.cancel(false);
                            }
                        }, flushExecutor);
                    }
                }
                existing = null;
            }
        }

        if (existing == null) {
            log.info("Creating new QueueManager for groupId={}, domainId={}, processingCycleId={}",
                    groupId, config.getDomainId(), config.getProcessingCycleId());

            QueueManagerImpl newManager = new QueueManagerImpl(
                    config, meterRegistry, mappingExecutor, flushExecutor, flushScheduler, flushSignalCallback
            );


            synchronized (INSTANCES) {
                WeakReference<QueueManagerImpl> currentRef = INSTANCES.get(groupId);
                QueueManagerImpl current = currentRef != null ? currentRef.get() : null;

                if (current != null) {
                    log.info("Another thread created QueueManager for groupId={} concurrently. Using theirs.", groupId);

                    CompletableFuture.runAsync(() -> {
                        if (newManager.flushTask != null) {
                            newManager.flushTask.cancel(false);
                        }
                    }, flushExecutor);

                    return current;
                }

                INSTANCES.put(groupId, new WeakReference<>(newManager));
                log.info("Installed new QueueManager for groupId={}", groupId);
                return newManager;
            }
        }

        throw new IllegalStateException("Failed to get or create QueueManager for groupId=" + groupId);
    }
}
