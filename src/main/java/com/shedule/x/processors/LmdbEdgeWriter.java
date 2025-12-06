package com.shedule.x.processors;

import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.basic.Murmur3;
import com.shedule.x.utils.graph.StoreUtility;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import org.springframework.beans.factory.annotation.Qualifier;
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
public class LmdbEdgeWriter implements EdgeWriter {

    private final LmdbEnvironment lmdb;
    private final ExecutorService writeExecutor;
    private final MeterRegistry meters;

    // Configuration (unchanged)
    private static final int WRITE_QUEUE_CAPACITY = 10000;
    private static final int MAX_BATCH_SIZE = 1024;
    private static final int MAX_BATCH_DRAIN = 256;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 100;

    private final BlockingQueue<WriteRequest> writeQueue = new LinkedBlockingQueue<>(WRITE_QUEUE_CAPACITY);
    private final Thread writerThread;
    private volatile boolean shutdownRequested = false;

    // Dedicated writer thread buffers
    private static final ThreadLocal<ByteBuffer> WRITER_KEY = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(512).order(ByteOrder.BIG_ENDIAN));
    private static final ThreadLocal<ByteBuffer> WRITER_VAL = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(1024).order(ByteOrder.BIG_ENDIAN));
    private static final ThreadLocal<ByteBuffer> WRITER_PREFIX = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64).order(ByteOrder.BIG_ENDIAN));

    public LmdbEdgeWriter(LmdbEnvironment lmdb,
                          @Qualifier("persistenceExecutor") ExecutorService writeExecutor,
                          MeterRegistry meters) {
        this.lmdb = lmdb;
        this.writeExecutor = writeExecutor;
        this.meters = meters;

        this.writerThread = new Thread(this::runWriter, "lmdb-edge-writer");
        this.writerThread.setDaemon(false);
        this.writerThread.start();

        log.info("LmdbEdgeWriter initialized with dedicated writer thread");
    }

    private static class WriteRequest {
        final List<GraphRecords.PotentialMatch> matches;
        final UUID groupId;
        final String cycleId;
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final long enqueuedTime = System.currentTimeMillis();

        WriteRequest(List<GraphRecords.PotentialMatch> matches, UUID groupId, String cycleId) {
            this.matches = matches;
            this.groupId = groupId;
            this.cycleId = cycleId;
        }
    }

    private void runWriter() {
        log.info("LMDB edge writer thread started");
        List<WriteRequest> batch = new ArrayList<>();

        while (!shutdownRequested || !writeQueue.isEmpty()) {
            try {
                WriteRequest req = writeQueue.poll(100, TimeUnit.MILLISECONDS);
                if (req != null) {
                    batch.add(req);
                    writeQueue.drainTo(batch, MAX_BATCH_DRAIN);
                }

                if (!batch.isEmpty()) {
                    processBatch(batch);
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in writer thread", e);
                batch.forEach(b -> b.future.completeExceptionally(e));
                batch.clear();
            }
        }

        if (!batch.isEmpty()) processBatch(batch);
        log.info("LMDB edge writer thread stopped");
    }

    private void processBatch(List<WriteRequest> batch) {
        List<WriteTask> tasks = new ArrayList<>();

        for (WriteRequest req : batch) {
            long wait = System.currentTimeMillis() - req.enqueuedTime;
            meters.timer("write_queue_wait_time").record(wait, TimeUnit.MILLISECONDS);

            List<List<GraphRecords.PotentialMatch>> chunks = partition(req.matches, MAX_BATCH_SIZE);
            for (var chunk : chunks) {
                tasks.add(new WriteTask(chunk, req.groupId, req.cycleId, req));
            }
        }

        tasks.forEach(this::executeWriteWithRetry);

        batch.forEach(req -> {
            if (!req.future.isDone()) req.future.complete(null);
        });

        log.debug("Processed batch of {} requests ({} tasks)", batch.size(), tasks.size());
    }

    private record WriteTask(List<GraphRecords.PotentialMatch> matches,
                             UUID groupId,
                             String cycleId,
                             WriteRequest original) {}

    private void executeWriteWithRetry(WriteTask task) {
        Exception last = null;
        for (int i = 1; i <= MAX_RETRY_ATTEMPTS; i++) {
            try {
                executeSingleWrite(task);
                meters.counter("write_success").increment();
                return;
            } catch (Exception e) {
                last = e;
                meters.counter("write_retry", "attempt", String.valueOf(i)).increment();
                if (i < MAX_RETRY_ATTEMPTS) {
                    try { Thread.sleep(RETRY_DELAY_MS * i); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); return; }
                }
            }
        }
        log.error("Write failed after {} attempts", MAX_RETRY_ATTEMPTS, last);
        meters.counter("write_failure").increment();
        task.original().future.completeExceptionally(last);
    }

    private void executeSingleWrite(WriteTask task) throws Exception {
        long start = System.nanoTime();
        try (Txn<ByteBuffer> txn = lmdb.env().txnWrite()) {
            Dbi<ByteBuffer> dbi = lmdb.edgeDbi();

            ByteBuffer kb = WRITER_KEY.get();
            ByteBuffer vb = WRITER_VAL.get();
            ByteBuffer prefixBuf = WRITER_PREFIX.get();

            prefixBuf.clear();
            StoreUtility.putUUID(prefixBuf, task.groupId);
            Murmur3.hash128To(prefixBuf, task.cycleId);
            prefixBuf.flip();
            ByteBuffer prefixRO = prefixBuf.asReadOnlyBuffer();

            for (var m : task.matches) {
                kb.clear();
                kb.put(prefixRO.duplicate());

                String a = m.getReferenceId(), b = m.getMatchedReferenceId();
                if (a.compareTo(b) > 0) { String t = a; a = b; b = t; }

                Murmur3.hash128To(kb, a + "\0" + b);
                kb.flip();

                vb.clear();
                vb.putFloat((float) m.getCompatibilityScore());
                StoreUtility.putUUID(vb, m.getDomainId());
                StoreUtility.putString(vb, m.getReferenceId());
                StoreUtility.putString(vb, m.getMatchedReferenceId());
                vb.flip();

                dbi.put(txn, kb, vb);
            }

            txn.commit();
            meters.counter("edges_written_total").increment(task.matches.size());
            meters.timer("write_transaction_time").record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public CompletableFuture<Void> enqueueWrite(List<GraphRecords.PotentialMatch> matches,
                                                UUID groupId,
                                                String cycleId) {
        WriteRequest req = new WriteRequest(matches, groupId, cycleId);
        if (!writeQueue.offer(req)) {
            meters.counter("write_queue_full").increment();
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(new RuntimeException("Write queue is full"));
            return f;
        }
        meters.gauge("write_queue_size", writeQueue.size());
        return req.future;
    }

    @Override
    public void shutdown() {
        log.info("Shutting down LmdbEdgeWriter");
        shutdownRequested = true;

        try {
            writerThread.join(10_000);
            if (writerThread.isAlive()) {
                log.warn("Writer thread didn't stop gracefully – interrupting");
                writerThread.interrupt();
                writerThread.join(5_000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for writer thread", e);
        }

        if (!writeQueue.isEmpty()) {
            List<WriteRequest> remaining = new ArrayList<>();
            int drained = writeQueue.drainTo(remaining);
            log.warn("Discarding {} pending write requests during shutdown", drained);

            remaining.forEach(request ->
                    request.future.completeExceptionally(
                            new RuntimeException("EdgePersistence shutting down")
                    )
            );
        }

        log.info("LmdbEdgeWriter shutdown complete");
    }
}