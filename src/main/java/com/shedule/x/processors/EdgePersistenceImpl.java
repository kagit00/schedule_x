package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.basic.Murmur3;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.shedule.x.utils.graph.StoreUtility.*;

@Component
@Slf4j
public class EdgePersistenceImpl implements EdgePersistence {
    private final LmdbEnvironment lmdb;
    private final ExecutorService writeExecutor;
    private final MeterRegistry meters;
    private final Semaphore writeSemaphore = new Semaphore(2, true);

    private static final ThreadLocal<ByteBuffer> KEY_BUFFER = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(512).order(ByteOrder.BIG_ENDIAN));
    private static final ThreadLocal<ByteBuffer> VAL_BUFFER = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(1024).order(ByteOrder.BIG_ENDIAN));
    private static final int KEY_REF_OFFSET = 20;

    private static final int KEY_MAT_OFFSET = 36;

    private final ThreadLocal<byte[]> tmp16Bytes = ThreadLocal.withInitial(() -> new byte[16]);

    public EdgePersistenceImpl(LmdbEnvironment lmdb,
                               @Qualifier("persistenceExecutor") ExecutorService writeExecutor,
                               MeterRegistry meters) {
        this.lmdb = lmdb;
        this.writeExecutor = writeExecutor;
        this.meters = meters;
    }


    @Override
    public void cleanEdges(UUID groupId) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> edgeDbi = lmdb.edgeDbi();
        Txn<ByteBuffer> txn = null;
        Cursor<ByteBuffer> cur = null;

        try {
            txn = env.txnWrite();
            cur = edgeDbi.openCursor(txn);

            ByteBuffer prefix = keyBuf().clear();
            putUUID(prefix, groupId);
            prefix.flip();

            boolean found = cur.get(prefix, GetOp.MDB_SET_RANGE);
            if (!found) {
                cur.close();
                txn.commit();
                return;
            }

            int deleted = 0;

            do {
                ByteBuffer k = cur.key();

                if (!matchesGroupPrefix(k, groupId)) break;

                cur.delete();
                deleted++;

            } while (cur.next());

            cur.close();
            txn.commit();

            meters.counter("edges_cleaned").increment(deleted);

        } catch (Exception e) {
            log.warn("Clean fail", e);
            if (txn != null) {
                try { txn.abort(); } catch (Exception ignore) {}
            }
        } finally {
            if (cur != null) {
                try {
                    cur.close();
                } catch (Exception ignore) {}
            }
            if (txn != null) {
                try {
                    txn.close();
                } catch (Exception ignore) {}
            }
        }
    }

    public CompletableFuture<Void> persistEdgesAsyncFallback(List<GraphRecords.PotentialMatch> matches,
                                                             String groupId, int chunkIndex, Throwable t) {
        log.warn("Persist failed: {}", t.getMessage());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> persistAsync(List<GraphRecords.PotentialMatch> matches, UUID groupId, int chunkIndex) {
        if (matches.isEmpty()) return CompletableFuture.completedFuture(null);

        return CompletableFuture.runAsync(() -> {
            try {
                if (!writeSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                    throw new TimeoutException("LMDB Write Semaphore Timeout");
                }

                Env<ByteBuffer> env = lmdb.env();
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    Dbi<ByteBuffer> dbi = lmdb.edgeDbi();
                    ByteBuffer kb = KEY_BUFFER.get();
                    ByteBuffer vb = VAL_BUFFER.get();

                    for (GraphRecords.PotentialMatch m : matches) {
                        kb.clear();
                        putUUID(kb, groupId); // 16 bytes
                        kb.putInt(chunkIndex); // 4 bytes
                        Murmur3.hash128To(kb, m.getReferenceId()); // 16 bytes
                        Murmur3.hash128To(kb, m.getMatchedReferenceId()); // 16 bytes
                        kb.flip();

                        vb.clear();
                        vb.putFloat((float) m.getCompatibilityScore());
                        putUUID(vb, m.getDomainId());
                        vb.flip();

                        dbi.put(txn, kb, vb);
                    }
                    txn.commit();
                    meters.counter("edges_written", "groupId", groupId.toString()).increment(matches.size());
                }
            } catch (Exception e) {
                log.error("LMDB write failed for groupId={}", groupId, e);
                throw new CompletionException(e);
            } finally {
                writeSemaphore.release();
            }
        }, writeExecutor);
    }

    @Override
    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId) {
        Env<ByteBuffer> env = lmdb.env();
        Txn<ByteBuffer> txn = env.txnRead();
        Dbi<ByteBuffer> dbi = lmdb.edgeDbi();

        try {
            var cursorIterable = dbi.iterate(txn, KeyRange.atLeast(makePrefix(groupId)));
            Iterator<CursorIterable.KeyVal<ByteBuffer>> kvIter = cursorIterable.iterator();

            Iterator<EdgeDTO> edgeIter = new Iterator<>() {
                EdgeDTO nextItem = null;
                boolean done = false;

                @Override
                public boolean hasNext() {
                    if (nextItem != null) return true;
                    if (done) return false;

                    nextItem = advance();
                    if (nextItem == null) {
                        done = true;
                        return false;
                    }
                    return true;
                }

                @Override
                public EdgeDTO next() {
                    if (!hasNext())
                        throw new NoSuchElementException();
                    EdgeDTO result = nextItem;
                    nextItem = null;
                    return result;
                }

                private EdgeDTO advance() {
                    while (kvIter.hasNext()) {
                        var kv = kvIter.next();
                        ByteBuffer key = kv.key();

                        if (!matchesPrefix(key, groupId))
                            return null;

                        ByteBuffer val = kv.val();

                        long msb = val.getLong(4);
                        long lsb = val.getLong(12);
                        if (msb == domainId.getMostSignificantBits() &&
                                lsb == domainId.getLeastSignificantBits()) {

                            return decodeEdge(key, val, groupId, domainId);
                        }
                    }
                    return null;
                }
            };



            Stream<EdgeDTO> stream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(edgeIter,
                            Spliterator.ORDERED | Spliterator.NONNULL),
                    false
            );


            return new AutoCloseableStream<>(stream, () -> {
                try {
                    cursorIterable.close();
                } finally {
                    txn.close();
                }
            });

        } catch (Exception e) {
            txn.close();
            throw new RuntimeException("Failed to stream edges", e);
        }
    }


    private ByteBuffer makePrefix(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocateDirect(16).order(ByteOrder.BIG_ENDIAN);
        putUUID(bb, uuid);
        bb.flip();
        return bb;
    }

    private EdgeDTO decodeEdge(ByteBuffer k, ByteBuffer v, UUID groupId, UUID domainId) {
        byte[] refBytes = new byte[16];
        byte[] matBytes = new byte[16];

        k.position(20); // Skip Group(16) + Chunk(4)
        k.get(refBytes);
        k.get(matBytes);

        String refId = Murmur3.toHex(refBytes);
        String matId = Murmur3.toHex(matBytes);
        float score = v.getFloat(0);

        return EdgeDTO.builder()
                .fromNodeHash(refId)
                .toNodeHash(matId)
                .score(score)
                .domainId(domainId)
                .groupId(groupId)
                .build();
    }

    private boolean matchesPrefix(ByteBuffer k, UUID groupId) {
        if (k.remaining() < 16) return false;
        return k.getLong(0) == groupId.getMostSignificantBits() &&
                k.getLong(8) == groupId.getLeastSignificantBits();
    }

    @Override
    public void close() throws Exception {

    }
}