package com.shedule.x.processors;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.config.factory.QuadFunction;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

@Slf4j
public class QueueManagerImpl {
    private static final ConcurrentHashMap<UUID, QueueManagerImpl> INSTANCES = new ConcurrentHashMap<>();
    private static final int MIN_FLUSH_INTERVAL_SECONDS = 1;
    private static final int MAX_FLUSH_INTERVAL_SECONDS = 60;

    @Getter private final UUID groupId;
    private final UUID domainId;
    private final String processingCycleId;
    private final MeterRegistry meterRegistry;
    private final ScheduledExecutorService flushScheduler;
    private final QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback;

    private final BlockingQueue<GraphRecords.PotentialMatch> memoryQueue;
    private final SegmentedDiskBuffer diskSpillBuffer;

    private final int maxBatchSize;
    private final AtomicInteger flushIntervalSeconds;
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    private final ReentrantLock flushLock = new ReentrantLock();

    private final Semaphore asyncFlushPermit;
    private final AtomicBoolean isSpilling = new AtomicBoolean(false);

    private volatile ScheduledFuture<?> flushTask;
    private volatile boolean closed = false;

    private final AtomicLong enqueueCount = new AtomicLong(0);
    private final AtomicLong dequeueCount = new AtomicLong(0);

    private QueueManagerImpl(
            QueueConfig config,
            MeterRegistry meterRegistry,
            ScheduledExecutorService flushScheduler,
            QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback,
            Path spillBasePath) throws IOException {

        this.config = config;
        this.groupId = config.getGroupId();
        this.domainId = config.getDomainId();
        this.processingCycleId = config.getProcessingCycleId();
        this.meterRegistry = meterRegistry;
        this.flushScheduler = flushScheduler;
        this.flushSignalCallback = flushSignalCallback;

        this.memoryQueue = new LinkedBlockingQueue<>(config.getCapacity());
        this.maxBatchSize = config.getMaxFinalBatchSize();

        this.diskSpillBuffer = new SegmentedDiskBuffer(spillBasePath.resolve("group_" + groupId), 50_000);

        this.flushIntervalSeconds = new AtomicInteger(Math.max(1, config.getFlushIntervalSeconds()));
        this.asyncFlushPermit = new Semaphore(1);

        registerMetrics();
        schedulePeriodicFlush();
    }

    @Getter private final QueueConfig config;

    public static QueueManagerImpl getOrCreate(
            QueueConfig config,
            MeterRegistry meterRegistry,
            ScheduledExecutorService flushScheduler,
            QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback,
            Path spillBasePath) {

        return INSTANCES.compute(config.getGroupId(), (k, existing) -> {
            if (existing != null) {
                if (!existing.processingCycleId.equals(config.getProcessingCycleId())) {
                    existing.close();
                } else {
                    return existing;
                }
            }
            try {
                return new QueueManagerImpl(config, meterRegistry, flushScheduler, flushSignalCallback, spillBasePath);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public boolean enqueue(GraphRecords.PotentialMatch match) {
        if (closed) return false;

        if (isSpilling.get()) {
            return addToSpill(match);
        }

        if (memoryQueue.offer(match)) {
            enqueueCount.incrementAndGet();
            return true;
        }

        isSpilling.set(true);
        log.info("Memory queue full for groupId={}. Switching to disk spill.", groupId);

        CompletableFuture.runAsync(this::drainAndFlush);

        return addToSpill(match);
    }

    private boolean addToSpill(GraphRecords.PotentialMatch match) {
        if (!config.isUseDiskSpill()) return false;
        try {
            diskSpillBuffer.write(match);
            return true;
        } catch (IOException e) {
            log.error("Spill failed", e);
            meterRegistry.counter("queue.spill.error", "groupId", groupId.toString()).increment();
            return false;
        }
    }

    private void schedulePeriodicFlush() {
        if (closed) return;
        flushTask = flushScheduler.schedule(this::drainAndFlush, flushIntervalSeconds.get(), TimeUnit.SECONDS);
    }

    private void drainAndFlush() {
        if (closed) return;

        if (!flushLock.tryLock()) {
            reschedule();
            return;
        }

        try {
            if (!asyncFlushPermit.tryAcquire()) {
                reschedule();
                return;
            }

            List<GraphRecords.PotentialMatch> batch = new ArrayList<>(maxBatchSize);

            // Priority 1: Drain Disk first (FIFO)
            if (diskSpillBuffer.size() > 0) {
                try {
                    diskSpillBuffer.readBatch(batch, maxBatchSize);
                } catch (IOException e) {
                    log.error("Error reading spill buffer", e);
                }
            }

            // Priority 2: If space remains, drain Memory
            // Only drain memory if we exhausted disk (to maintain rough order)
            if (batch.size() < maxBatchSize && diskSpillBuffer.size() == 0) {
                isSpilling.set(false); // Disk is empty, revert to memory mode
                memoryQueue.drainTo(batch, maxBatchSize - batch.size());
            }

            if (batch.isEmpty()) {
                asyncFlushPermit.release();
                reschedule();
                return;
            }

            flushSignalCallback.apply(groupId, domainId, batch.size(), processingCycleId)
                    .whenComplete((v, ex) -> {
                        asyncFlushPermit.release(); // Release DB permit

                        if (ex != null) {
                            log.error("Flush failed for {} items. Re-queuing.", batch.size(), ex);
                            batch.forEach(this::addToSpill);
                        } else {
                            dequeueCount.addAndGet(batch.size());
                            lastFlushTime.set(System.currentTimeMillis());
                        }
                        reschedule();
                    });

        } finally {
            flushLock.unlock();
        }
    }

    private void reschedule() {
        if (!closed && !Thread.currentThread().isInterrupted()) {
            int delay = (!memoryQueue.isEmpty() || diskSpillBuffer.size() > 0) ? 0 : flushIntervalSeconds.get();

            // Avoid tight loops with 0 delay if asyncPermit is exhausted, check state
            if (delay == 0 && asyncFlushPermit.availablePermits() == 0) delay = 1;

            flushScheduler.schedule(this::drainAndFlush, delay, TimeUnit.SECONDS);
        }
    }

    public CompletableFuture<Void> flushNow(QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> callback) {
        return CompletableFuture.completedFuture(null);
    }

    public void close() {
        closed = true;
        if (flushTask != null) flushTask.cancel(false);

        try {
            drainAndFlush();
            diskSpillBuffer.close();
        } catch (Exception e) {
            log.warn("Error closing queue manager", e);
        }
        INSTANCES.remove(groupId);
    }

    private void registerMetrics() {
        Gauge.builder("queue.memory.size", memoryQueue, Collection::size)
                .tag("groupId", groupId.toString()).register(meterRegistry);
        Gauge.builder("queue.disk.size", diskSpillBuffer, SegmentedDiskBuffer::size)
                .tag("groupId", groupId.toString()).register(meterRegistry);
    }

    public static QueueManagerImpl getExisting(UUID groupId) {
        return INSTANCES.get(groupId);
    }

    public static void remove(UUID groupId) {
        QueueManagerImpl manager = INSTANCES.remove(groupId);
        if (manager != null) {
            manager.close();
        }
    }

    public static void removeAll() {
        // Create a copy of keys to avoid concurrent modification issues during iteration
        Set<UUID> keys = new HashSet<>(INSTANCES.keySet());
        for (UUID key : keys) {
            remove(key);
        }
    }

    public static CompletableFuture<Void> flushAllQueuesAsync(
            QuadFunction<UUID, UUID, String, Integer, CompletableFuture<Void>> callback) {

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        INSTANCES.forEach((groupId, manager) -> {
            CompletableFuture<Void> f = CompletableFuture.supplyAsync(() -> {
                List<GraphRecords.PotentialMatch> batch = manager.drainBatch(Integer.MAX_VALUE);
                if (batch.isEmpty()) return CompletableFuture.completedFuture((Void) null);

                return callback.apply(
                        manager.getGroupId(),
                        manager.domainId,
                        manager.processingCycleId,
                        batch.size()
                );
            }, manager.flushScheduler).thenCompose(Function.identity());

            futures.add(f);
        });

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    public List<GraphRecords.PotentialMatch> drainBatch(int limit) {
        List<GraphRecords.PotentialMatch> batch = new ArrayList<>(Math.min(limit, maxBatchSize));

        flushLock.lock();
        try {
            if (diskSpillBuffer.size() > 0) {
                try {
                    diskSpillBuffer.readBatch(batch, limit);
                } catch (IOException e) {
                    log.error("Failed to read from spill buffer for groupId={}", groupId, e);
                }
            }

            int remaining = limit - batch.size();
            if (remaining > 0) {
                memoryQueue.drainTo(batch, remaining);
            }

            if (diskSpillBuffer.size() == 0 && isSpilling.get()) {
                isSpilling.set(false);
            }

            if (!batch.isEmpty()) {
                dequeueCount.addAndGet(batch.size());
                lastFlushTime.set(System.currentTimeMillis());
            }

            return batch;

        } finally {
            flushLock.unlock();
        }
    }


    public boolean hasSpillData() {
        return diskSpillBuffer.size() > 0;
    }
}