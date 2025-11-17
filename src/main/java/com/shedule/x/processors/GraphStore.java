package com.shedule.x.processors;

import static com.shedule.x.utils.graph.StoreUtility.matchesGroupPrefix;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_WRITEMAP;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.AlgorithmUtils;
import com.shedule.x.utils.basic.Murmur3;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.lmdbjava.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.StreamSupport;

@Slf4j
@Component
public class GraphStore implements AutoCloseable {
    @Value("${graph.store.map-size:68719476736}") // 64 GB default
    private long maxDbSize;

    private static final int BATCH_BASE = 1_000;
    private static final int LOCK_STRIPES = 16_384; // 2^14
    private static final int MAX_KEY_BUF = 128;
    private static final int MAX_VAL_BUF = 512;
    private static final long BUCKET_TTL_NS = 86_400_000_000_000L; // 24h
    private static final int MAX_BUCKET_IDS = 40_000;

    // ====================== LMDB ======================
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> edgeDbi;
    private Dbi<ByteBuffer> lshDbi;

    // ====================== THREADING ======================
    private final ExecutorService asyncPool;
    private final MeterRegistry meterRegistry;
    private final String dbPath;
    private final AtomicLong totalBucketEntries = new AtomicLong(0);
    private final StampedLock[] stripeLocks = new StampedLock[LOCK_STRIPES];

    // ====================== BUFFER POOLS ======================
    private final ThreadLocal<ByteBuffer> keyBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(MAX_KEY_BUF));
    private final ThreadLocal<ByteBuffer> valBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(MAX_VAL_BUF));
    private final ThreadLocal<ByteBuffer> tmpBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(64));
    private final ThreadLocal<ByteBuffer> lshKeyBuf =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(12).order(ByteOrder.BIG_ENDIAN));
    private final ThreadLocal<long[]> mergeBuf =
            ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 4]);
    private final ThreadLocal<ByteBuffer> bucketBuf =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(MAX_BUCKET_IDS * 16 + 64));
    private final ThreadLocal<long[]> radixTmp =
            ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 2]);



    private final Semaphore writeSemaphore = new Semaphore(1);
    private static final int KEY_REF_OFFSET = 20;   // group(16) + chunk(4)
    private static final int KEY_MAT_OFFSET = 36;   // group(16) + chunk(4) + ref(16)

    private final ThreadLocal<byte[]> tmp16Bytes = ThreadLocal.withInitial(() -> new byte[16]);

    private final ScheduledExecutorService syncScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "lmdb-sync-thread");
                t.setDaemon(true);
                return t;
            });


    private ByteBuffer lshKeyBuffer(int table, int band) {
        ByteBuffer b = lshKeyBuf.get();
        b.clear();
        b.putInt(0x4C534800).putInt(table).putInt(band);
        b.flip();
        return b;
    }

    public GraphStore(
            @Qualifier("persistenceExecutor") ExecutorService asyncPool,
            MeterRegistry meterRegistry,
            @Value("${graph.store.path:/app/graph-store}") String dbPath) {
        this.asyncPool = asyncPool;
        this.meterRegistry = meterRegistry;
        this.dbPath = dbPath;
    }

    @PostConstruct
    public void init() throws IOException {
        validateAndCreatePath();
        initLocks();
        initLMDB();
        bindMetrics();
        log.info("GraphStore ready at {}", new File(dbPath).getAbsolutePath());

        syncScheduler.scheduleAtFixedRate(() -> {
            try {
                env.sync(false);
            } catch (Exception e) {
                log.warn("LMDB periodic sync failed", e);
            }
        }, 30, 30, TimeUnit.SECONDS);

        log.info("GraphStore ready at {}", new File(dbPath).getAbsolutePath());
    }

    @PreDestroy
    @Override
    public void close() {
        syncScheduler.shutdownNow();
        asyncPool.shutdownNow();
        try {
            if (env != null) env.sync(true);
        } catch (Exception ignored) {}
        if (edgeDbi != null) edgeDbi.close();
        if (lshDbi != null) lshDbi.close();
        if (env != null) env.close();
    }


    private void validateAndCreatePath() throws IOException {
        File dir = new File(dbPath);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create " + dir);
        }
        if (!dir.canWrite()) throw new IOException("No write on " + dir);
    }

    private void initLocks() {
        for (int i = 0; i < LOCK_STRIPES; i++) stripeLocks[i] = new StampedLock();
    }

    private void initLMDB() {
        env = Env.create()
                .setMapSize(maxDbSize)
                .setMaxDbs(2)
                .setMaxReaders(4096)
                .open(new File(dbPath), EnvFlags.MDB_MAPASYNC, EnvFlags.MDB_NOSYNC);

        edgeDbi = env.openDbi("edges", MDB_CREATE);
        lshDbi = env.openDbi("lsh", MDB_CREATE);
    }

    private void bindMetrics() {
        meterRegistry.gauge("lmdb_map_size_bytes", this, gs -> env.stat().entries * 64L);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmGcMetrics().bindTo(meterRegistry);
    }


    public CompletableFuture<Void> persistEdgesAsync(List<GraphRecords.PotentialMatch> matches,
                                                     UUID groupId, int chunkIndex) {
        if (matches.isEmpty()) return CompletableFuture.completedFuture(null);
        int batch = Math.min(BATCH_BASE, matches.size());
        List<CompletableFuture<Void>> subs = new ArrayList<>();
        for (List<GraphRecords.PotentialMatch> part : ListUtils.partition(matches, batch)) {
            subs.add(CompletableFuture.runAsync(() ->
                    writeEdgeBatch(part, groupId, chunkIndex), asyncPool));
        }
        return CompletableFuture.allOf(subs.toArray(new CompletableFuture[0]));
    }

    private void writeEdgeBatch(List<GraphRecords.PotentialMatch> batch, UUID groupId, int chunkIndex) {
        try {
            writeSemaphore.acquire();

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer kb = keyBuf();
                ByteBuffer vb = valBuf();

                for (GraphRecords.PotentialMatch m : batch) {
                    kb.clear();
                    putUUID(kb, groupId);
                    kb.putInt(chunkIndex);
                    Murmur3.hash128To(kb, m.getReferenceId());
                    Murmur3.hash128To(kb, m.getMatchedReferenceId());
                    kb.flip();

                    vb.clear();
                    vb.putFloat((float) m.getCompatibilityScore());
                    putUUID(vb, m.getDomainId());
                    vb.flip();

                    edgeDbi.put(txn, kb, vb);
                }

                txn.commit();
                meterRegistry.counter("edges_written").increment(batch.size());
            }

        } catch (Exception e) {
            meterRegistry.counter("edges_write_err").increment(batch.size());
            log.warn("Edge batch fail", e);

        } finally {
            writeSemaphore.release();
        }
    }


    public AutoCloseableStream<Edge> streamEdges(UUID domainId, UUID groupId) {
        Txn<ByteBuffer> txn = env.txnRead();
        ByteBuffer prefix = keyBuf().clear();
        putUUID(prefix, groupId);
        prefix.flip();
        CursorIterable<ByteBuffer> iter = edgeDbi.iterate(txn, KeyRange.atLeast(prefix));
        Iterator<CursorIterable.KeyVal<ByteBuffer>> it = iter.iterator();
        java.util.stream.Stream<Edge> stream = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(new Iterator<Edge>() {
                            Edge next = fetchNext(it, domainId, groupId);
                            @Override public boolean hasNext() { return next != null; }
                            @Override public Edge next() {
                                if (next == null) throw new NoSuchElementException();
                                Edge r = next;
                                next = fetchNext(it, domainId, groupId);
                                return r;
                            }
                        }, Spliterator.NONNULL), false)
                .onClose(() -> { iter.close(); txn.close(); });
        return new AutoCloseableStream<>(stream);
    }

    private Edge fetchNext(Iterator<CursorIterable.KeyVal<ByteBuffer>> it,
                           UUID domainId, UUID groupId) {
        byte[] tmp16 = tmp16Bytes.get();
        while (it.hasNext()) {
            CursorIterable.KeyVal<ByteBuffer> kv = it.next();
            ByteBuffer k = kv.key();

            if (k.getLong(0)  != groupId.getMostSignificantBits() ||
                    k.getLong(8)  != groupId.getLeastSignificantBits()) {
                return null;
            }

            ByteBuffer v = kv.val();
            UUID dom = new UUID(v.getLong(4), v.getLong(12));
            if (!dom.equals(domainId)) continue;

            float score = v.getFloat(0);

            ByteBuffer kd = k.duplicate().order(ByteOrder.BIG_ENDIAN);

            kd.position(KEY_REF_OFFSET);
            kd.get(tmp16, 0, 16);
            String refHex = Murmur3.toHex(tmp16, 0, 16);

            kd.position(KEY_MAT_OFFSET);
            kd.get(tmp16, 0, 16);
            String matHex = Murmur3.toHex(tmp16, 0, 16);

            GraphRecords.PotentialMatch pm = new GraphRecords.PotentialMatch(
                    refHex, matHex, score, groupId, domainId);
            return GraphRequestFactory.toEdge(pm);
        }
        return null;
    }

    public void addToBucket(int tableIdx, int band, Collection<UUID> nodeIds) {
        if (nodeIds.isEmpty() || nodeIds.size() > MAX_BUCKET_IDS) return;

        long hash = ((long) tableIdx << 32) | (band & 0xFFFFFFFFL);
        int stripe = stripe(hash);
        StampedLock lock = stripeLocks[stripe];
        long stamp = lock.writeLock();
        try {
            ByteBuffer keyBufDirect = lshKeyBuffer(tableIdx, band);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer existingVal = lshDbi.get(txn, keyBufDirect);
                long[] existing = existingVal == null ? new long[0] : decodeBucket(existingVal);

                long[] incoming = new long[nodeIds.size() * 2];
                int pos = 0;
                for (UUID id : nodeIds) {
                    incoming[pos++] = id.getMostSignificantBits();
                    incoming[pos++] = id.getLeastSignificantBits();
                }

                sortByPairs(incoming);

                long[] merged = mergeAndDedupPairs(existing, incoming);
                if (merged.length > MAX_BUCKET_IDS * 2) {
                    merged = Arrays.copyOf(merged, MAX_BUCKET_IDS * 2);
                }

                ByteBuffer valDirect = encodeBucketDirect(merged);
                keyBufDirect.rewind();
                lshDbi.put(txn, keyBufDirect, valDirect);
                txn.commit();

                totalBucketEntries.addAndGet(merged.length / 2L - existing.length / 2L);
            }
        } catch (Exception e) {
            log.warn("LSH add fail {}-{}", tableIdx, band, e);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public Set<UUID> getBucket(int tableIdx, int band) {
        ByteBuffer keyBufDirect = lshKeyBuffer(tableIdx, band);
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer val = lshDbi.get(txn, keyBufDirect);
            if (val == null) return Set.of();
            long[] raw = decodeBucket(val);
            Set<UUID> set = new HashSet<>(raw.length / 2);
            for (int i = 0; i < raw.length; i += 2) {
                set.add(new UUID(raw[i], raw[i + 1]));
            }
            return Set.copyOf(set);
        } catch (Exception e) {
            log.warn("LSH get fail", e);
            return Set.of();
        }
    }

    private void sortByPairs(long[] pairs) {
        int n = pairs.length;
        if (n <= 2) return;
        if ((n & 1) != 0) throw new IllegalArgumentException("pairs length must be even");

        long[] tmp = radixTmp.get();
        if (tmp.length < pairs.length) {
            tmp = new long[pairs.length];
            radixTmp.set(tmp);
        }

        AlgorithmUtils.radixSortPairs(pairs, tmp);
    }


    private static int comparePair(long aMsb, long aLsb, long bMsb, long bLsb) {
        int c = Long.compare(aMsb, bMsb);
        if (c != 0) return c;
        return Long.compare(aLsb, bLsb);
    }

    private long[] mergeAndDedupPairs(long[] existing, long[] incoming) {
        if ((existing.length & 1) != 0 || (incoming.length & 1) != 0)
            throw new IllegalArgumentException("pair arrays must have even length");

        int i = 0, j = 0;
        int n1 = existing.length, n2 = incoming.length;

        long[] tmp = mergeBuf.get();
        int w = 0;

        while (i < n1 && j < n2) {
            long aMsb = existing[i], aLsb = existing[i + 1];
            long bMsb = incoming[j], bLsb = incoming[j + 1];
            int cmp = comparePair(aMsb, aLsb, bMsb, bLsb);
            if (cmp < 0) {
                tmp[w++] = aMsb; tmp[w++] = aLsb;
                i += 2;
            } else if (cmp > 0) {
                tmp[w++] = bMsb; tmp[w++] = bLsb;
                j += 2;
            } else {
                tmp[w++] = aMsb; tmp[w++] = aLsb;
                i += 2; j += 2;
            }
        }
        while (i < n1) {
            tmp[w++] = existing[i++]; tmp[w++] = existing[i++];
        }
        while (j < n2) {
            tmp[w++] = incoming[j++]; tmp[w++] = incoming[j++];
        }

        return w == tmp.length ? tmp : Arrays.copyOf(tmp, w);
    }


    private ByteBuffer encodeBucketDirect(long[] ids) {
        ByteBuffer buf = bucketBuf.get();
        buf.clear();
        buf.putLong(System.nanoTime());
        buf.putInt(ids.length / 2);
        for (long l : ids) buf.putLong(l);
        buf.flip();
        return buf;
    }


    private long[] decodeBucket(ByteBuffer data) {
        if (data.remaining() < 12) return new long[0];
        ByteBuffer dup = data.duplicate().order(ByteOrder.BIG_ENDIAN);
        long ts = dup.getLong(0);
        if (System.nanoTime() - ts > BUCKET_TTL_NS) return new long[0];
        int count = dup.getInt(8);
        long[] arr = new long[count * 2];
        dup.position(12);
        for (int i = 0; i < arr.length; i++) arr[i] = dup.getLong();
        return arr;
    }


    public void cleanEdges(UUID groupId) {
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

            meterRegistry.counter("edges_cleaned").increment(deleted);

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


    public void clearAllBuckets() {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            Cursor<ByteBuffer> cur = lshDbi.openCursor(txn);
            if (!cur.first()) return;
            int cleared = 0;
            do {
                cur.delete();
                cleared++;
            } while (cur.next());
            txn.commit();
            totalBucketEntries.set(0);
            log.info("Cleared {} LSH buckets", cleared);
        } catch (Exception e) {
            log.warn("Clear buckets fail", e);
        }
    }

    public CompletableFuture<Void> persistEdgesAsyncFallback(List<GraphRecords.PotentialMatch> matches,
                                                             String groupId, int chunkIndex, Throwable t) {
        log.warn("Persist failed: {}", t.getMessage());
        return CompletableFuture.completedFuture(null);
    }

    private int stripe(long hash) { return (int) ((hash >>> 16) & (LOCK_STRIPES - 1)); }
    private ByteBuffer keyBuf() { ByteBuffer b = keyBuf.get(); b.clear(); return b; }
    private ByteBuffer valBuf() { ByteBuffer b = valBuf.get(); b.clear(); return b; }
    private void putUUID(ByteBuffer buf, UUID id) {
        buf.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
    }
}