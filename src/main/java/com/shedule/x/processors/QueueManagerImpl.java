package com.shedule.x.processors;

import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.ref.WeakReference;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
    private static final ConcurrentHashMap<String, WeakReference<QueueManagerImpl>> INSTANCES = new ConcurrentHashMap<>();
    private static final int REDUCED_MAX_FINAL_BATCH_SIZE = 10_000;
    private static final long TTL_SECONDS = 86_400;
    private final AtomicLong lastFlushedQueueSize = new AtomicLong(0);
    private static final int DEBOUNCE_WINDOW_MS = 1_000;
    private final ScheduledExecutorService flushScheduler;
    private static final int METRICS_REPORT_INTERVAL_SECONDS = 30;
    private static final ScheduledExecutorService GLOBAL_SCHEDULER = Executors.newScheduledThreadPool(8, r -> {
        Thread t = new Thread(r, "queue-manager-global-scheduler");
        t.setDaemon(true);
        return t;
    });

    private final BlockingQueue<GraphRecords.PotentialMatch> queue;
    private final UUID domainId;
    private final String processingCycleId;
    private final AtomicLong enqueueCount;
    private final AtomicLong dequeueCount;
    private final int capacity;
    private final double drainWarningThreshold;
    private final int boostBatchFactor;
    private final int maxFinalBatchSize;
    private final MeterRegistry meterRegistry;
    private final String groupId;
    private final AtomicInteger flushIntervalSeconds;
    private final ExecutorService mappingExecutor;
    private final ExecutorService flushExecutor;
    private final QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback;
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

    private QueueManagerImpl(
            int capacity, UUID domainId, String processingCycleId, MeterRegistry meterRegistry,
            String groupId, int flushIntervalSeconds, double drainWarningThreshold,
            int boostBatchFactor, int maxFinalBatchSize, ExecutorService mappingExecutor,
            ExecutorService flushExecutor, ScheduledExecutorService flushScheduler,
            QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback
    ) {
        this.capacity = capacity;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.domainId = Objects.requireNonNull(domainId, "domainId must not be null");
        this.processingCycleId = Objects.requireNonNull(processingCycleId, "processingCycleId must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.groupId = Objects.requireNonNull(groupId, "groupId must not be null");
        this.flushIntervalSeconds = new AtomicInteger(flushIntervalSeconds);
        this.drainWarningThreshold = drainWarningThreshold;
        this.boostBatchFactor = boostBatchFactor;
        this.maxFinalBatchSize = maxFinalBatchSize;
        this.mappingExecutor = Objects.requireNonNull(mappingExecutor, "mappingExecutor must not be null");
        this.flushExecutor = Objects.requireNonNull(flushExecutor, "flushExecutor must not be null");
        this.flushSignalCallback = Objects.requireNonNull(flushSignalCallback, "flushSignalCallback must not be null");
        this.enqueueCount = new AtomicLong(0);
        this.dequeueCount = new AtomicLong(0);
        this.creationTime = System.currentTimeMillis();
        this.flushScheduler = Objects.requireNonNull(flushScheduler, "flushScheduler must not be null");
        this.periodicFlushSemaphore = new Semaphore(2, true);
        this.blockingFlushSemaphore = new Semaphore(2, true);
        this.boostedFlushSemaphore = new Semaphore(1, true);

        Gauge.builder("match_queue_size", queue, BlockingQueue::size)
                .tag("groupId", groupId)
                .register(meterRegistry);
        Gauge.builder("match_queue_fill_ratio", queue, q -> (double) q.size() / capacity)
                .tag("groupId", groupId)
                .register(meterRegistry);

        startPeriodicFlush();
        startInstanceEviction();
        scheduleInstanceTTLEviction();

        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(QueueManagerImpl::removeAll, "QueueManagerShutdown"));
    }

    public long getQueueSize() {
        return queue.size();
    }

    public int getFlushInterval() {
        return flushIntervalSeconds.get();
    }

    public void setFlushInterval(int newIntervalSeconds) {
        long now = System.currentTimeMillis();
        if (now - lastFlushIntervalSetAt < DEBOUNCE_WINDOW_MS) {
            log.info("Debounced setFlushInterval {}s for groupId={} due to recent update", newIntervalSeconds, groupId);
            return;
        }
        if (newIntervalSeconds <= 0) {
            log.warn("Invalid flush interval {}s for groupId={}, ignoring", newIntervalSeconds, groupId);
            return;
        }

        try {
            if (scheduleLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                try {
                    int oldInterval = flushIntervalSeconds.getAndSet(newIntervalSeconds);
                    lastFlushIntervalSetAt = now;
                    log.info("Updated flush interval from {}s to {}s for groupId={}", oldInterval, newIntervalSeconds, groupId);
                    if (flushTask != null) {
                        flushTask.cancel(false);
                    }
                    scheduleGeneration.incrementAndGet();
                    startPeriodicFlush();
                } finally {
                    scheduleLock.unlock();
                }
            } else {
                log.warn("Could not acquire scheduleLock to set flush interval for groupId={}", groupId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while trying to acquire scheduleLock for groupId={}", groupId, e);
        }
    }

    public boolean enqueue(GraphRecords.PotentialMatch match) {
        try {
            if (!queue.offer(match, 1, TimeUnit.SECONDS)) {
                log.warn("Failed to enqueue match for groupId={} due to full queue", groupId);
                return false;
            }
            enqueueCount.incrementAndGet();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted enqueuing match for groupId={}: {}", groupId, e.getMessage());
            return false;
        }
    }

    private void startPeriodicFlush() {
        long generation = scheduleGeneration.get();
        flushTask = flushScheduler.scheduleWithFixedDelay(() -> {
            if (scheduleGeneration.get() != generation) {
                log.debug("Skipping stale periodic flush task for groupId={}, generation={}", groupId, generation);
                return;
            }
            try {
                int currentInterval = flushIntervalSeconds.get();
                int queueSize = queue.size();
                long lastSize = lastFlushedQueueSize.get();
                if (queueSize > 0 && (queueSize >= lastSize * 1.1 || queueSize >= REDUCED_MAX_FINAL_BATCH_SIZE)) {
                    int batchSize = Math.min(queueSize, REDUCED_MAX_FINAL_BATCH_SIZE);
                    log.debug("Periodic flush for groupId={}, queueSize={}, batchSize={}", groupId, queueSize, batchSize);
                    try {
                        if (!periodicFlushSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                            log.warn("Timeout acquiring periodicFlushSemaphore for groupId={}", groupId);
                            meterRegistry.counter("semaphore_acquire_timeout", "groupId", groupId).increment();
                            // Optionally return or skip flush since semaphore not acquired
                            return;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Interrupted acquiring periodicFlushSemaphore for groupId={}", groupId);
                        return;
                    }

                    CompletableFuture.runAsync(() ->
                                    flushSignalCallback.apply(groupId, domainId, batchSize, processingCycleId)
                                            .orTimeout(60, TimeUnit.SECONDS)
                                            .whenComplete((v, ex) -> {
                                                if (ex != null) {
                                                    log.error("Periodic flush failed for groupId={}: {}", groupId, ex.getMessage());
                                                } else {
                                                    lastFlushedQueueSize.set(queueSize);
                                                    meterRegistry.counter("graph_builder_flushes", "groupId", groupId).increment();
                                                }
                                            }), flushExecutor)
                            .whenComplete((v, e) -> periodicFlushSemaphore.release())
                            .exceptionally(e -> {
                                log.error("Periodic flush submission failed for groupId={}: {}", groupId, e.getMessage());
                                periodicFlushSemaphore.release();
                                return null;
                            });
                } else {
                    log.debug("Skipping periodic flush for groupId={} as queue is empty or growth insufficient (size={}, last={})", groupId, queueSize, lastSize);
                }
                reportQueueMetrics();
                checkQueueHealth();
            } catch (Exception e) {
                log.error("Error in periodic flush task for groupId={}: {}", groupId, e.getMessage());
            }
        }, flushIntervalSeconds.get() / 2, flushIntervalSeconds.get(), TimeUnit.SECONDS);
    }

    public static void flushQueueBlocking(String groupId, QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> flushCallback) {
        WeakReference<QueueManagerImpl> ref = INSTANCES.get(groupId);
        QueueManagerImpl manager = ref != null ? ref.get() : null;
        if (manager == null || manager.getQueue().isEmpty()) {
            return;
        }
        log.info("Attempting to flush {} matches for groupId={}, processingCycleId={}",
                manager.getQueue().size(), groupId, manager.processingCycleId);
        boolean acquired = false;
        try {
            acquired = manager.blockingFlushSemaphore.tryAcquire(30, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timeout acquiring blockingFlushSemaphore for groupId={}", groupId);
                return;
            }

            flushCallback.apply(groupId, manager.domainId,
                            Math.min(manager.getQueue().size(), manager.maxFinalBatchSize),
                            manager.processingCycleId)
                    .orTimeout(30, TimeUnit.SECONDS)
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            log.error("Blocking flush failed for groupId={}, processingCycleId={}: {}.",
                                    groupId, manager.processingCycleId, ex.getMessage());
                        } else {
                            log.info("Successfully flushed matches for groupId={}, processingCycleId={}",
                                    groupId, manager.processingCycleId);
                        }
                    }).whenComplete((v, e) -> manager.blockingFlushSemaphore.release())
                    .get(30, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted acquiring blockingFlushSemaphore for groupId={}: {}", groupId, e.getMessage());
            if (acquired) {
                manager.blockingFlushSemaphore.release();
            }
        } catch (ExecutionException | TimeoutException e) {
            log.error("Flush operation failed or timed out for groupId={}: {}", groupId, e.getMessage());
            if (acquired) {
                manager.blockingFlushSemaphore.release();
            }
        } catch (Exception e) {
            log.error("Blocking flush failed for groupId={}, processingCycleId={}: {}", groupId, manager.processingCycleId, e.getMessage());
        }
    }

    public static void flushAllQueuesBlocking(QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> flushCallback) {
        INSTANCES.forEach((groupId, ref) -> {
            try {
                CompletableFuture.runAsync(() -> flushQueueBlocking(groupId, flushCallback))
                        .orTimeout(30, TimeUnit.SECONDS)
                        .exceptionally(e -> {
                            log.error("Flush timeout or error for groupId={}: {}", groupId, e.getMessage());
                            return null;
                        }).get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed to initiate flush for groupId={}: {}", groupId, e.getMessage());
            }
        });
    }

    private void startInstanceEviction() {
        GLOBAL_SCHEDULER.scheduleWithFixedDelay(() -> {
            long now = System.currentTimeMillis();
            synchronized (INSTANCES) {
                INSTANCES.entrySet().removeIf(entry -> {
                    WeakReference<QueueManagerImpl> ref = entry.getValue();
                    QueueManagerImpl manager = ref.get();
                    if (manager == null || now - manager.creationTime > TTL_SECONDS * 1000) {
                        if (manager != null) {
                            log.info("Evicting QueueManager for groupId={} due to TTL expiration", manager.groupId);
                            remove(manager.groupId);
                        }
                        return true;
                    }
                    return false;
                });
            }
        }, TTL_SECONDS / 2, TTL_SECONDS / 2, TimeUnit.SECONDS);
    }

    private void scheduleInstanceTTLEviction() {
        flushScheduler.schedule(() -> {
            long now = System.currentTimeMillis();
            synchronized (INSTANCES) {
                if (now - creationTime > TTL_SECONDS * 1000) {
                    log.info("Evicting QueueManager for groupId={} due to TTL expiration", groupId);
                    remove(groupId);
                }
            }
        }, TTL_SECONDS, TimeUnit.SECONDS);
    }

    public void checkQueueHealth() {
        double fillRatio = (double) queue.size() / capacity;
        if (fillRatio > drainWarningThreshold && boostedDrainInProgress.compareAndSet(false, true)) {
            log.warn("Queue for groupId={} at {}% capacity (size={}/{}), triggering boosted drain",
                    groupId, String.format("%.1f", fillRatio * 100), queue.size(), capacity);
            meterRegistry.counter("queue_drain_warnings_total", "groupId", groupId).increment();
            int boostedBatchSize = Math.min(queue.size(), REDUCED_MAX_FINAL_BATCH_SIZE * boostBatchFactor);
            if (!boostedFlushSemaphore.tryAcquire()) {
                log.warn("Failed to acquire boostedFlushSemaphore for groupId={}", groupId);
                boostedDrainInProgress.set(false);
                return;
            }
            try {
                CompletableFuture.runAsync(() ->
                                flushSignalCallback.apply(groupId, domainId, boostedBatchSize, processingCycleId)
                                        .orTimeout(30, TimeUnit.SECONDS)
                                        .exceptionally(e -> {
                                            log.error("Boosted drain failed for groupId={}: {}", groupId, e.getMessage());
                                            return null;
                                        })
                                        .whenComplete((v, e) -> {
                                            boostedDrainInProgress.set(false);
                                            boostedFlushSemaphore.release();
                                        }), flushExecutor)
                        .exceptionally(e -> {
                            log.error("Boosted drain submission failed for groupId={}: {}", groupId, e.getMessage());
                            boostedDrainInProgress.set(false);
                            boostedFlushSemaphore.release();
                            return null;
                        });
            } catch (Exception e) {
                log.error("Error during boosted drain for groupId={}: {}", groupId, e.getMessage());
                boostedDrainInProgress.set(false);
                boostedFlushSemaphore.release();
            }
        }
        if (fillRatio > 0) {
            log.debug("Queue health for groupId={}: size={}/{}, fillRatio={}",
                    groupId, queue.size(), capacity, String.format("%.3f", fillRatio));
        }
    }

    private String hashGroupId(String groupId) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(groupId.getBytes(StandardCharsets.UTF_8));
            return String.format("%016x", new BigInteger(1, hash)).substring(0, 8);
        } catch (Exception e) {
            log.warn("Failed to hash groupId={}, using first 8 chars", groupId);
            return groupId.length() > 8 ? groupId.substring(0, 8) : groupId;
        }
    }

    public static QueueManagerImpl getOrCreate(
            String groupId, UUID domainId, String processingCycleId, int capacity,
            MeterRegistry meterRegistry, int flushIntervalSeconds, double drainWarningThreshold,
            int boostBatchFactor, int maxFinalBatchSize, ExecutorService mappingExecutor,
            ExecutorService flushExecutor, ScheduledExecutorService flushScheduler,
            QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback
    ) {
        synchronized (INSTANCES) {
            return INSTANCES.compute(groupId, (key, ref) -> {
                QueueManagerImpl existing = ref != null ? ref.get() : null;
                if (existing == null) {
                    INSTANCES.remove(key, ref);
                } else {
                    if (!existing.domainId.equals(domainId)) {
                        throw new IllegalStateException("GroupId " + key + " associated with domainId " + existing.domainId + ", cannot use " + domainId);
                    }
                    if (!existing.processingCycleId.equals(processingCycleId)) {
                        throw new IllegalStateException("GroupId " + key + " associated with processingCycleId " + existing.processingCycleId + ", cannot use " + processingCycleId);
                    }
                    return ref;
                }
                QueueManagerImpl newManager = new QueueManagerImpl(
                        capacity, domainId, processingCycleId, meterRegistry, key, flushIntervalSeconds,
                        drainWarningThreshold, boostBatchFactor, maxFinalBatchSize,
                        mappingExecutor, flushExecutor, flushScheduler, flushSignalCallback
                );
                return new WeakReference<>(newManager);
            }).get();
        }
    }

    public static void remove(String groupId) {
        synchronized (INSTANCES) {
            WeakReference<QueueManagerImpl> ref = INSTANCES.remove(groupId);
            QueueManagerImpl manager = ref != null ? ref.get() : null;
            if (manager != null) {
                log.info("Removed QueueManager for groupId={}", groupId);
                if (manager.flushTask != null) {
                    manager.flushTask.cancel(false);
                }
            }
        }
    }

    public static void removeAll() {
        synchronized (INSTANCES) {
            for (String groupId : INSTANCES.keySet()) {
                remove(groupId);
            }
        }
    }

    public void reportQueueMetrics() {
        try {
            if (metricsLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                try {
                    meterRegistry.counter("match_queue_enqueued_total", "groupId", groupId)
                            .increment(enqueueCount.getAndSet(0));
                    meterRegistry.counter("match_queue_dequeued_total", "groupId", groupId)
                            .increment(dequeueCount.getAndSet(0));
                } finally {
                    metricsLock.unlock();
                }
            } else {
                log.warn("Could not acquire metricsLock to report queue metrics for groupId={}", groupId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            log.error("Interrupted while trying to acquire metricsLock for groupId={}", groupId, e);
        }
    }
}