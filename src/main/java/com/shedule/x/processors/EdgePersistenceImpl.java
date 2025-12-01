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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
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
    private static final ThreadLocal<ByteBuffer> TEMP_PREFIX = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(64).order(ByteOrder.BIG_ENDIAN));

    public EdgePersistenceImpl(LmdbEnvironment lmdb,
                               @Qualifier("persistenceExecutor") ExecutorService writeExecutor,
                               MeterRegistry meters) {
        this.lmdb = lmdb;
        this.writeExecutor = writeExecutor;
        this.meters = meters;
    }


    @Override
    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId, String cycleId) {
        ByteBuffer prefix = makePrefix(groupId, cycleId);
        return streamEdgesInternal(domainId, prefix, key -> matchesPrefix(key, groupId, cycleId));
    }

    @Override
    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId) {
        ByteBuffer prefix = makePrefix(groupId);
        return streamEdgesInternal(domainId, prefix, key -> matchesPrefix(key, groupId));
    }

    @Override
    public void cleanEdges(UUID groupId, String cycleId) {
        deleteByPrefix(makePrefix(groupId, cycleId), key -> matchesPrefix(key, groupId, cycleId));
    }

    @Override
    public CompletableFuture<Void> persistAsync(List<GraphRecords.PotentialMatch> matches,
                                                UUID groupId, int chunkIndex, String cycleId) {
        if (matches.isEmpty()) return CompletableFuture.completedFuture(null);

        return CompletableFuture.runAsync(() -> {
            try {
                if (!writeSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                    throw new TimeoutException("LMDB Write Semaphore Timeout");
                }

                try (Txn<ByteBuffer> txn = lmdb.env().txnWrite()) {
                    Dbi<ByteBuffer> dbi = lmdb.edgeDbi();
                    ByteBuffer kb = KEY_BUFFER.get();
                    ByteBuffer vb = VAL_BUFFER.get();

                    for (GraphRecords.PotentialMatch m : matches) {
                        kb.clear();
                        putUUID(kb, groupId);
                        Murmur3.hash128To(kb, cycleId);
                        kb.putInt(chunkIndex);
                        Murmur3.hash128To(kb, m.getReferenceId());
                        Murmur3.hash128To(kb, m.getMatchedReferenceId());
                        kb.flip();

                        vb.clear();
                        vb.putFloat((float) m.getCompatibilityScore());

                        putUUID(vb, m.getDomainId());
                        putString(vb, m.getReferenceId());
                        putString(vb, m.getMatchedReferenceId());

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


    private AutoCloseableStream<EdgeDTO> streamEdgesInternal(UUID domainId, ByteBuffer prefix,
                                                             java.util.function.Predicate<ByteBuffer> prefixMatcher) {
        Env<ByteBuffer> env = lmdb.env();
        Txn<ByteBuffer> txn = env.txnRead();
        Dbi<ByteBuffer> dbi = lmdb.edgeDbi();

        try {
            var cursorIterable = dbi.iterate(txn, KeyRange.atLeast(prefix));
            Iterator<CursorIterable.KeyVal<ByteBuffer>> kvIter = cursorIterable.iterator();

            Iterator<EdgeDTO> edgeIter = new Iterator<>() {
                EdgeDTO nextItem = null;
                boolean done = false;

                @Override public boolean hasNext() {
                    if (nextItem != null) return true;
                    if (done) return false;
                    nextItem = advance();
                    if (nextItem == null) done = true;
                    return nextItem != null;
                }

                @Override public EdgeDTO next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    EdgeDTO result = nextItem;
                    nextItem = null;
                    return result;
                }

                private EdgeDTO advance() {
                    while (kvIter.hasNext()) {
                        var kv = kvIter.next();
                        ByteBuffer key = kv.key();
                        if (!prefixMatcher.test(key)) return null;

                        ByteBuffer val = kv.val();
                        if (!domainMatches(val, domainId)) continue;

                        return decodeEdge(key, val, domainId);
                    }
                    return null;
                }
            };

            Stream<EdgeDTO> stream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(edgeIter, Spliterator.ORDERED | Spliterator.NONNULL),
                    false);

            return new AutoCloseableStream<>(stream, () -> {
                try { cursorIterable.close(); }
                finally { txn.close(); }
            });

        } catch (Exception e) {
            txn.close();
            throw new RuntimeException("Failed to stream edges", e);
        }
    }

    private void deleteByPrefix(ByteBuffer prefix, Predicate<ByteBuffer> matcher) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> dbi = lmdb.edgeDbi();

        ByteBuffer scratchKey = KEY_BUFFER.get();
        ByteBuffer scratchVal = VAL_BUFFER.get();

        try (Txn<ByteBuffer> txn = env.txnWrite()) {

            try (Cursor<ByteBuffer> cur = dbi.openCursor(txn)) {

                boolean found = cur.get(prefix, GetOp.MDB_SET_RANGE);
                int deleted = 0;

                while (found) {
                    ByteBuffer curKey = cur.key();
                    if (curKey == null) break;

                    if (curKey.remaining() > prefix.remaining() &&
                            !curKey.duplicate().limit(prefix.remaining()).equals(prefix)) {
                        break;
                    }

                    int cmp = comparePrefixOrder(prefix, curKey);
                    if (cmp > 0) {
                        break;
                    }

                    scratchKey.clear();
                    scratchKey.put(curKey.duplicate());
                    scratchKey.flip();

                    if (matcher.test(scratchKey)) {
                        cur.delete();
                        deleted++;
                    }
                    found = cur.next();
                }

                meters.counter("edges_cleaned").increment(deleted);

            }

            txn.commit();

        } catch (Exception e) {
            log.warn("Clean fail", e);
        }
    }

    private int comparePrefixOrder(ByteBuffer prefix, ByteBuffer key) {
        int pLen = prefix.remaining();
        int kLen = key.remaining();
        int min = Math.min(pLen, kLen);

        ByteBuffer p = prefix.duplicate();
        ByteBuffer k = key.duplicate();

        for (int i = 0; i < min; i++) {
            int pb = p.get() & 0xFF;
            int kb = k.get() & 0xFF;
            if (pb != kb) {
                return Integer.compare(kb, pb);
            }
        }

        if (kLen < pLen) return -1;
        if (kLen > pLen) return 0;
        return 0;
    }



    private boolean domainMatches(ByteBuffer val, UUID domainId) {
        long msb = val.getLong(4);
        long lsb = val.getLong(12);
        return msb == domainId.getMostSignificantBits() && lsb == domainId.getLeastSignificantBits();
    }

    private EdgeDTO decodeEdge(ByteBuffer key, ByteBuffer val, UUID domainId) {
        val.position(0);

        float score = val.getFloat();
        UUID domain = getUUID(val);

        String fromRef = readString(val);
        String toRef = readString(val);

        return EdgeDTO.builder()
                .fromNodeHash(fromRef)
                .toNodeHash(toRef)
                .score(score)
                .domainId(domain)
                .groupId(extractGroupId(key))
                .build();
    }


    private UUID extractGroupId(ByteBuffer key) {
        long msb = key.getLong(0);
        long lsb = key.getLong(8);
        return new UUID(msb, lsb);
    }

    // ==================== VARARGS PREFIX ====================

    private ByteBuffer makePrefix(Object... parts) {
        ByteBuffer bb = TEMP_PREFIX.get().clear();
        for (Object part : parts) {
            if (part instanceof UUID uuid) {
                putUUID(bb, uuid);
            } else if (part instanceof String str) {
                Murmur3.hash128To(bb, str);
            } else {
                throw new IllegalArgumentException("Unsupported prefix part: " + part);
            }
        }
        bb.flip();
        return bb;
    }

    private boolean matchesPrefix(ByteBuffer key, Object... expectedParts) {
        if (key.remaining() < expectedParts.length * 16) return false;

        int pos = 0;
        for (Object part : expectedParts) {
            if (part instanceof UUID uuid) {
                if (key.getLong(pos) != uuid.getMostSignificantBits() ||
                        key.getLong(pos + 8) != uuid.getLeastSignificantBits()) {
                    return false;
                }
                pos += 16;
            } else if (part instanceof String str) {
                byte[] expectedHash = Murmur3.hash128(str);
                for (int i = 0; i < 16; i++) {
                    if (key.get(pos + i) != expectedHash[i]) return false;
                }
                pos += 16;
            }
        }
        return true;
    }

    @Override
    public void close() throws Exception { }

    public static String readString(ByteBuffer bb) {
        int len = bb.getInt();
        byte[] arr = new byte[len];
        bb.get(arr);
        return new String(arr, StandardCharsets.UTF_8);
    }
}