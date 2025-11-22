package com.shedule.x.processors;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
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
import java.util.stream.Stream;

@Slf4j
public class QueueManagerImpl {
    // Changed to Strong references. Lifecycle should be managed explicitly via close().
    private static final ConcurrentHashMap<UUID, QueueManagerImpl> INSTANCES = new ConcurrentHashMap<>();

    private static final int MIN_FLUSH_INTERVAL_SECONDS = 1;
    private static final int MAX_FLUSH_INTERVAL_SECONDS = 60;

    @Getter private final UUID groupId;
    private final UUID domainId;
    private final String processingCycleId;
    private final MeterRegistry meterRegistry;
    private final ScheduledExecutorService flushScheduler;
    // Callback signature
    private final QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback;

    private final BlockingQueue<GraphRecords.PotentialMatch> memoryQueue;
    private final SegmentedDiskBuffer diskSpillBuffer;

    private final int maxBatchSize;
    private final AtomicInteger flushIntervalSeconds;
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    private final ReentrantLock flushLock = new ReentrantLock();

    // Flow control
    private final Semaphore asyncFlushPermit; // Limits concurrent async DB calls
    private final AtomicBoolean isSpilling = new AtomicBoolean(false);

    private volatile ScheduledFuture<?> flushTask;
    private volatile boolean closed = false;

    // Metrics
    private final AtomicLong enqueueCount = new AtomicLong(0);
    private final AtomicLong dequeueCount = new AtomicLong(0);

    @FunctionalInterface
    public interface QuadFunction<T, U, V, W, R> {
        R apply(T t, U u, V v, W w);
    }

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

        // Initialize Segmented Disk Buffer
        this.diskSpillBuffer = new SegmentedDiskBuffer(spillBasePath.resolve("group_" + groupId), 50_000);

        this.flushIntervalSeconds = new AtomicInteger(Math.max(1, config.getFlushIntervalSeconds()));
        // Allow only 1 active flush to DB at a time to preserve partial ordering and reduce DB pressure
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
                    existing.close(); // Cycle changed, recycle
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

        // 1. If we are already spilling, go straight to disk to preserve order and clear memory
        if (isSpilling.get()) {
            return addToSpill(match);
        }

        // 2. Try Memory
        if (memoryQueue.offer(match)) {
            enqueueCount.incrementAndGet();
            return true;
        }

        // 3. Memory Full -> Enable Spilling Mode
        isSpilling.set(true);
        log.info("Memory queue full for groupId={}. Switching to disk spill.", groupId);

        // Trigger a flush immediately to start clearing space
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

        // Ensure we don't stack up flushes if the DB is slow
        if (!flushLock.tryLock()) {
            reschedule();
            return;
        }

        try {
            // 1. Acquire permit to talk to DB. If DB is busy, we wait (backpressure).
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

            // Send to Processor
            flushSignalCallback.apply(groupId, domainId, batch.size(), processingCycleId)
                    .whenComplete((v, ex) -> {
                        asyncFlushPermit.release(); // Release DB permit

                        if (ex != null) {
                            log.error("Flush failed for {} items. Re-queuing.", batch.size(), ex);
                            // Simple retry logic: put back in spill
                            batch.forEach(this::addToSpill);
                        } else {
                            dequeueCount.addAndGet(batch.size());
                            lastFlushTime.set(System.currentTimeMillis());
                        }
                        // Schedule next run immediately (if queue is full) or later
                        reschedule();
                    });

        } finally {
            flushLock.unlock();
        }
    }

    private void reschedule() {
        if (!closed && !Thread.currentThread().isInterrupted()) {
            // Adaptive timing: If we found work, run again soon. If empty, wait.
            int delay = (memoryQueue.size() > 0 || diskSpillBuffer.size() > 0) ? 0 : flushIntervalSeconds.get();

            // Avoid tight loops with 0 delay if asyncPermit is exhausted, check state
            if (delay == 0 && asyncFlushPermit.availablePermits() == 0) delay = 1;

            flushScheduler.schedule(this::drainAndFlush, delay, TimeUnit.SECONDS);
        }
    }

    public CompletableFuture<Void> flushNow(QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> callback) {
        // Implementation for manual blocking flush (e.g., on close)
        // Similar logic to drainAndFlush but blocking.
        return CompletableFuture.completedFuture(null); // Simplified for brevity
    }

    public void close() {
        closed = true;
        if (flushTask != null) flushTask.cancel(false);

        try {
            // Final best-effort flush
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

    // ==========================================
    // SEGMENTED DISK BUFFER (The Performance Fix)
    // ==========================================
    private static class SegmentedDiskBuffer implements AutoCloseable {
        private final Path directory;
        private final int itemsPerSegment;
        private final AtomicLong totalSize = new AtomicLong(0);

        private long currentWriteSegmentId = 0;
        private long currentReadSegmentId = 0;

        private ObjectOutputStream currentWriter;
        private ObjectInputStream currentReader;

        // To handle ObjectOutputStream header issue
        private boolean isWriterFresh = true;

        public SegmentedDiskBuffer(Path directory, int itemsPerSegment) throws IOException {
            this.directory = directory;
            this.itemsPerSegment = itemsPerSegment;
            Files.createDirectories(directory);
            recoverState();
        }

        private void recoverState() throws IOException {
            // Logic to scan directory, find min/max segment IDs to restore
            // currentReadSegmentId and currentWriteSegmentId
            // Simplified: assume empty for new runs
            cleanDirectory();
            openWriter();
        }

        private void cleanDirectory() throws IOException {
            try (Stream<Path> files = Files.list(directory)) {
                files.forEach(p -> { try { Files.delete(p); } catch (IOException e) {} });
            }
        }

        public synchronized void write(GraphRecords.PotentialMatch match) throws IOException {
            // Rotate file if too large
            if (Files.size(getSegmentPath(currentWriteSegmentId)) > itemsPerSegment * 1024L) { // Rough byte estimate or count
                rotateWriter();
            }

            currentWriter.writeObject(match);
            // reset to prevent memory leaks in ObjectOutputStream reference cache
            currentWriter.reset();
            totalSize.incrementAndGet();
        }

        private Path getSegmentPath(long id) {
            return directory.resolve(String.format("segment_%d.bin", id));
        }

        private void openWriter() throws IOException {
            Path p = getSegmentPath(currentWriteSegmentId);
            // Use appending, but we must handle the Header issue
            boolean exists = Files.exists(p);
            FileOutputStream fos = new FileOutputStream(p.toFile(), true);
            BufferedOutputStream bos = new BufferedOutputStream(fos);

            if (exists) {
                // Appending: Use subclass that doesn't write header
                currentWriter = new AppendingObjectOutputStream(bos);
            } else {
                // New file: Standard writes header
                currentWriter = new ObjectOutputStream(bos);
            }
        }

        private void rotateWriter() throws IOException {
            currentWriter.close();
            currentWriteSegmentId++;
            openWriter();
        }

        public synchronized void readBatch(List<GraphRecords.PotentialMatch> batch, int limit) throws IOException {
            if (totalSize.get() == 0) return;

            int count = 0;
            while (count < limit && totalSize.get() > 0) {
                if (currentReader == null) {
                    Path p = getSegmentPath(currentReadSegmentId);
                    if (!Files.exists(p)) {
                        // Should not happen if size > 0, but safety check
                        if (currentReadSegmentId < currentWriteSegmentId) {
                            currentReadSegmentId++;
                            continue;
                        } else {
                            break;
                        }
                    }
                    currentReader = new ObjectInputStream(new BufferedInputStream(new FileInputStream(p.toFile())));
                }

                try {
                    GraphRecords.PotentialMatch m = (GraphRecords.PotentialMatch) currentReader.readObject();
                    batch.add(m);
                    count++;
                    totalSize.decrementAndGet();
                } catch (EOFException e) {
                    // End of current segment
                    currentReader.close();
                    currentReader = null;
                    Files.deleteIfExists(getSegmentPath(currentReadSegmentId)); // Delete processed file
                    currentReadSegmentId++;
                } catch (ClassNotFoundException e) {
                    throw new IOException("Class mismatch", e);
                }
            }
        }

        public long size() { return totalSize.get(); }

        @Override
        public synchronized void close() throws IOException {
            if (currentWriter != null) currentWriter.close();
            if (currentReader != null) currentReader.close();
        }
    }

    // Helper to fix the Corruption Bug
    private static class AppendingObjectOutputStream extends ObjectOutputStream {
        public AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }
        @Override
        protected void writeStreamHeader() throws IOException {
            // Do not write header (magic number) when appending
            reset();
        }
    }

    // ---------------------------------------------------------
    // STATIC MANAGEMENT METHODS
    // ---------------------------------------------------------

    /**
     * Retrieves an existing QueueManager without creating a new one.
     * Used by the Processor to check if a specific group has data pending.
     */
    public static QueueManagerImpl getExisting(UUID groupId) {
        return INSTANCES.get(groupId);
    }

    /**
     * Removes the manager from the registry and ensures resources are closed.
     * usage: Called during cleanup or after final save.
     */
    public static void remove(UUID groupId) {
        QueueManagerImpl manager = INSTANCES.remove(groupId);
        if (manager != null) {
            manager.close();
        }
    }

    /**
     * Removes all managers and closes them.
     * Usage: Application shutdown.
     */
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
            // We trigger a manual flush/save via the callback
            CompletableFuture<Void> f = CompletableFuture.supplyAsync(() -> {
                // 1. Drain everything
                List<GraphRecords.PotentialMatch> batch = manager.drainBatch(Integer.MAX_VALUE);
                if (batch.isEmpty()) return CompletableFuture.completedFuture((Void) null);

                // 2. Pass to callback (Processor::savePendingMatchesAsync logic)
                return callback.apply(
                        manager.getGroupId(),
                        manager.domainId,
                        manager.processingCycleId, // Ensure order matches your QuadFunction
                        batch.size()
                );
            }, manager.flushScheduler).thenCompose(Function.identity());

            futures.add(f);
        });

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    // ---------------------------------------------------------
    // INSTANCE DATA METHODS
    // ---------------------------------------------------------

    /**
     * Drains up to 'limit' items from the queue.
     * Priority:
     * 1. Disk Spill Buffer (to maintain roughly FIFO order if we spilled previously)
     * 2. Memory Queue (if disk is empty or we need to fill the batch)
     */
    public List<GraphRecords.PotentialMatch> drainBatch(int limit) {
        List<GraphRecords.PotentialMatch> batch = new ArrayList<>(Math.min(limit, maxBatchSize));

        // Use lock to coordinate with the internal periodic flush task
        flushLock.lock();
        try {
            // 1. Try to read from Disk first (if exists)
            if (diskSpillBuffer.size() > 0) {
                try {
                    // Note: ensure SegmentedDiskBuffer.readBatch is visible/accessible
                    diskSpillBuffer.readBatch(batch, limit);
                } catch (IOException e) {
                    log.error("Failed to read from spill buffer for groupId={}", groupId, e);
                    // We continue to try memory even if disk fails to avoid total stall
                }
            }

            // 2. Fill remaining space from Memory
            int remaining = limit - batch.size();
            if (remaining > 0) {
                memoryQueue.drainTo(batch, remaining);
            }

            // 3. Update State
            // If disk is now empty, we can turn off the "isSpilling" flag
            // so new incoming items go directly to memory again.
            if (diskSpillBuffer.size() == 0 && isSpilling.get()) {
                isSpilling.set(false);
            }

            // 4. Metrics
            if (!batch.isEmpty()) {
                dequeueCount.addAndGet(batch.size());
                lastFlushTime.set(System.currentTimeMillis());
            }

            return batch;

        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Helper check used by cleanup processes
     */
    public boolean hasSpillData() {
        return diskSpillBuffer.size() > 0;
    }
}