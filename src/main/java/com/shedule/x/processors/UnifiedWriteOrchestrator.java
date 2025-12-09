package com.shedule.x.processors;

import com.shedule.x.dto.EdgeWriteRequest;
import com.shedule.x.dto.EdgeWriteTask;
import com.shedule.x.dto.LshWriteRequest;
import com.shedule.x.dto.WriteRequest;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.basic.Murmur3;
import com.shedule.x.utils.graph.EdgeKeyBuilder;
import com.shedule.x.utils.graph.StoreUtility;
import io.micrometer.core.instrument.MeterRegistry;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static com.shedule.x.utils.db.BatchUtils.partition;

@Component
@Slf4j
public class UnifiedWriteOrchestrator {

    private final LmdbEnvironment lmdb;
    private final LshBucketManager lshBucketManager;
    private final MeterRegistry meters;

    // Configuration
    private static final int QUEUE_CAPACITY = 10000;
    private static final int MAX_BATCH_DRAIN = 256;
    private static final int MAX_RETRY = 3;
    private static final long RETRY_DELAY_MS = 100;

    private final BlockingQueue<WriteRequest> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread writerThread = new Thread(this::run, "unified-lmdb-writer");
    private volatile boolean shutdown = false;

    // Thread-local buffers for writer thread
    private static final ThreadLocal<ByteBuffer> W_KEY = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(512).order(ByteOrder.BIG_ENDIAN));
    private static final ThreadLocal<ByteBuffer> W_VAL = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(1024).order(ByteOrder.BIG_ENDIAN));
    private static final ThreadLocal<ByteBuffer> W_PREFIX = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(64).order(ByteOrder.BIG_ENDIAN));

    public UnifiedWriteOrchestrator(LmdbEnvironment lmdb,
                                    @Lazy LshBucketManager lshBucketManager,
                                    MeterRegistry meters) {
        this.lmdb = lmdb;
        this.lshBucketManager = lshBucketManager;
        this.meters = meters;

        writerThread.setDaemon(false);
        writerThread.start();
        log.info("UnifiedWriteOrchestrator started – handles Edge + LSH writes");
    }


    public CompletableFuture<Void> enqueueEdgeWrite(List<GraphRecords.PotentialMatch> matches,
                                                    UUID groupId,
                                                    String cycleId) {
        EdgeWriteRequest req = new EdgeWriteRequest(matches, groupId, cycleId);
        return enqueue(req, "edge");
    }

    public CompletableFuture<Void> enqueueLshWrite(Long2ObjectMap<List<UUID>> chunk) {
        LshWriteRequest req = new LshWriteRequest(chunk);
        return enqueue(req, "lsh");
    }

    private CompletableFuture<Void> enqueue(WriteRequest req, String type) {
        if (!queue.offer(req)) {
            meters.counter("write_queue_full", "type", type).increment();
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("Write queue full"));
            return failed;
        }
        meters.gauge("write_queue_size", queue.size());
        return req.future();
    }


    private void run() {
        log.info("Unified writer thread started");
        List<WriteRequest> batch = new ArrayList<>();

        while (!shutdown || !queue.isEmpty()) {
            try {
                WriteRequest first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    queue.drainTo(batch, MAX_BATCH_DRAIN);
                }

                if (!batch.isEmpty()) {
                    processBatch(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Writer thread interrupted");
                break;
            } catch (Exception e) {
                log.error("Unexpected error in writer thread", e);
                batch.forEach(r -> r.future().completeExceptionally(e));
                batch.clear();
            }
        }

        if (!batch.isEmpty()) processBatch(batch);
        log.info("Unified writer thread stopped");
    }


    private void processBatch(List<WriteRequest> batch) {
        List<EdgeWriteRequest> edgeRequests = new ArrayList<>();
        List<LshWriteRequest> lshRequests = new ArrayList<>();

        for (WriteRequest r : batch) {
            long waitMs = System.currentTimeMillis() - r.enqueuedTime();
            meters.timer("write_queue_wait_time", "type", r.type().name().toLowerCase())
                    .record(waitMs, TimeUnit.MILLISECONDS);

            if (r instanceof EdgeWriteRequest e) edgeRequests.add(e);
            else if (r instanceof LshWriteRequest l) lshRequests.add(l);
        }

        lshRequests.forEach(this::writeLshWithRetry);

        if (!edgeRequests.isEmpty()) {
            writeEdgesBatch(edgeRequests);
        }

        batch.forEach(r -> {
            if (!r.future().isDone()) {
                r.future().complete(null);
            }
        });
    }

    private void writeLshWithRetry(LshWriteRequest req) {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRY; i++) {
            try {
                executeLshWrite(req.chunk());
                meters.counter("lsh.write_success").increment();
                return;
            } catch (Exception e) {
                last = e;
                meters.counter("lsh.write_retry", "attempt", String.valueOf(i)).increment();
                if (i < MAX_RETRY) sleep(RETRY_DELAY_MS * i);
            }
        }
        log.error("LSH write failed after {} attempts", MAX_RETRY, last);
        meters.counter("lsh.write_failure").increment();
        req.future().completeExceptionally(last);
    }

    private void executeLshWrite(Long2ObjectMap<List<UUID>> chunk) {
        long start = System.nanoTime();
        try (Txn<ByteBuffer> txn = lmdb.env().txnWrite()) {
            int buckets = 0;
            for (var entry : chunk.long2ObjectEntrySet()) {
                long composite = entry.getLongKey();
                int band = (int) (composite >>> 32);
                int bucketHash = (int) composite;
                lshBucketManager.mergeAndWriteBucket(txn, band, bucketHash, entry.getValue());
                buckets++;
            }
            txn.commit();
            meters.timer("lsh.txn.duration").record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            meters.summary("lsh.txn.buckets_per_txn").record(buckets);
            meters.counter("lsh.buckets_written_total").increment(buckets);
        }
    }

    private void writeEdgesBatch(List<EdgeWriteRequest> requests) {
        List<EdgeWriteTask> tasks = new ArrayList<>();
        for (EdgeWriteRequest r : requests) {
            partition(r.matches(), 1024).forEach(chunk ->
                    tasks.add(new EdgeWriteTask(chunk, r.groupId(), r.cycleId(), r)));
        }

        for (EdgeWriteTask task : tasks) {
            for (int i = 1; i <= MAX_RETRY; i++) {
                try {
                    executeEdgeWrite(task);
                    meters.counter("edge.write_success").increment();
                    break;
                } catch (Exception e) {
                    if (i == MAX_RETRY) {
                        meters.counter("edge.write_failure").increment();
                        task.originalRequest().future().completeExceptionally(e);
                    } else {
                        sleep(RETRY_DELAY_MS * i);
                    }
                }
            }
        }
    }

    private void executeEdgeWrite(EdgeWriteTask task) {
        long start = System.nanoTime();
        try (Txn<ByteBuffer> txn = lmdb.env().txnWrite()) {
            Dbi<ByteBuffer> dbi = lmdb.edgeDbi();
            ByteBuffer valBuf = W_VAL.get();

            for (GraphRecords.PotentialMatch m : task.matches()) {

                ByteBuffer keyBuf = EdgeKeyBuilder.build(
                        task.groupId(),
                        task.cycleId(),
                        m.getReferenceId(),
                        m.getMatchedReferenceId()
                );

                valBuf.clear();
                valBuf.putFloat((float) m.getCompatibilityScore());
                StoreUtility.putUUID(valBuf, m.getDomainId());
                StoreUtility.putString(valBuf, m.getReferenceId());
                StoreUtility.putString(valBuf, m.getMatchedReferenceId());
                valBuf.flip();

                dbi.put(txn, keyBuf, valBuf);
            }

            txn.commit();
            meters.counter("edges_written_total").increment(task.matches().size());
            meters.timer("edge.txn.duration").record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }


    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }


    public void shutdown() {
        log.info("Shutting down UnifiedWriteOrchestrator");
        shutdown = true;

        try {
            writerThread.join(10_000);
            if (writerThread.isAlive()) {
                log.warn("Writer thread still alive – interrupting");
                writerThread.interrupt();
                writerThread.join(5_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        List<WriteRequest> remaining = new ArrayList<>();
        queue.drainTo(remaining);
        if (!remaining.isEmpty()) {
            log.warn("Discarding {} pending writes during shutdown", remaining.size());
            remaining.forEach(r -> r.future()
                    .completeExceptionally(new IllegalStateException("Service shutting down")));
        }

        log.info("UnifiedWriteOrchestrator shutdown complete");
    }
}