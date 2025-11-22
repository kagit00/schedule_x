package com.shedule.x.processors;

import com.shedule.x.config.factory.NodePriorityProvider;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static com.shedule.x.utils.graph.StoreUtility.*;

@Slf4j
@Component
public class LshBucketManagerImpl implements LshBucketManager {
    private static final int LOCK_STRIPES = 16_384;
    private static final int MAX_BUCKET_IDS = 40_000;
    private static final int TARGET_SIZE = 2_000;
    private static final long BUCKET_TTL_NS = 86_400_000_000_000L;
    private static final int LSH_BUCKET_TARGET_SIZE = 2_000;
    private final AtomicLong totalBucketEntries = new AtomicLong(0);

    @Autowired
    private NodePriorityProvider nodePriorityProvider;

    @Autowired
    private LmdbEnvironment lmdb;

    private final StampedLock[] stripeLocks = new StampedLock[LOCK_STRIPES];

    private final ThreadLocal<ByteBuffer> keyBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(12).order(ByteOrder.BIG_ENDIAN));
    private final ThreadLocal<ByteBuffer> valBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(MAX_BUCKET_IDS * 16 + 64));
    private final ThreadLocal<long[]> mergeBuf = ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 4]);
    private final ThreadLocal<ByteBuffer> lshKeyBuf =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(12).order(ByteOrder.BIG_ENDIAN));
    private final ThreadLocal<long[]> radixTmp =
            ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 2]);

    private final ThreadLocal<ByteBuffer> lshValBuf =
            ThreadLocal.withInitial(() ->
                    ByteBuffer.allocateDirect(MAX_BUCKET_IDS * 16 + 64)
                            .order(ByteOrder.BIG_ENDIAN));


    @PostConstruct
    void init() {
        IntStream.range(0, LOCK_STRIPES).forEach(i -> stripeLocks[i] = new StampedLock());
    }

    @Override
    public Set<UUID> getBucket(int tableIdx, int band) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();
        ByteBuffer keyBufDirect = lshKeyBuf.get();
        keyBufDirect.clear();
        keyBufDirect.putInt(0x4C534800).putInt(tableIdx).putInt(band);
        keyBufDirect.flip();

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

    @Override
    public int getBucketSize(int tableIdx, int band) {
        return getBucket(tableIdx, band).size();
    }

    @Override
    public void addToBucket(int tableIdx, int band, Collection<UUID> nodeIds) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();
        if (nodeIds.isEmpty() || nodeIds.size() > MAX_BUCKET_IDS) return;

        long hash = ((long) tableIdx << 32) | (band & 0xFFFFFFFFL);
        int stripe = stripe(hash);
        StampedLock lock = stripeLocks[stripe];
        long stamp = lock.writeLock();
        try {
            final ByteBuffer keyBufDirect = lshKeyBuf.get();
            keyBufDirect.clear();
            keyBufDirect.putInt(0x4C534800).putInt(tableIdx).putInt(band); // Magic + table + band
            keyBufDirect.flip();

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer existingVal = lshDbi.get(txn, keyBufDirect);

                // Parse existing bucket
                long[] existing = (existingVal == null)
                        ? new long[0]
                        : decodeBucket(existingVal);

                // Convert incoming UUIDs to sorted long-pairs
                long[] incoming = toLongArray(nodeIds);
                sortByPairs(incoming);

                // Merge + dedup
                long[] merged = mergeAndDedupPairs(existing, incoming);
                int finalSize = merged.length / 2;

                // Trim under lock if too large
                if (finalSize > LSH_BUCKET_TARGET_SIZE) {
                    merged = trimByPriority(merged);
                    finalSize = merged.length / 2;
                }

                // Required LMDB value bytes: UUID pairs + 4-byte count + 8-byte MSB-LSB marker if used
                int requiredBytes = merged.length * Long.BYTES + 12;

                ByteBuffer valDirect = lshValBuf.get();
                if (valDirect.capacity() < requiredBytes) {
                    // Grow buffer safely and replace ThreadLocal reference
                    valDirect = ByteBuffer.allocateDirect(requiredBytes * 2)
                            .order(ByteOrder.BIG_ENDIAN);
                    lshValBuf.set(valDirect);
                }

                // Encode back to LMDB value
                valDirect.clear();
                valDirect = encodeBucket(merged);

                // Write to LMDB
                keyBufDirect.rewind();
                lshDbi.put(txn, keyBufDirect, valDirect);
                txn.commit();

                long added = finalSize - (existing.length / 2);
                totalBucketEntries.addAndGet(added);

                if (finalSize == LSH_BUCKET_TARGET_SIZE && added < 0) {
                    log.info("Bucket {}-{} auto-trimmed to {} nodes",
                            tableIdx, band, LSH_BUCKET_TARGET_SIZE);
                }
            }
        } catch (Exception e) {
            log.warn("LSH add failed {}-{}", tableIdx, band, e);
        } finally {
            lock.unlockWrite(stamp);
        }
    }


    @Override
    public void removeFromBucket(int tableIdx, int band, UUID nodeId) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();
        long hash = ((long) tableIdx << 32) | (band & 0xFFFFFFFFL);
        int stripe = stripe(hash);
        StampedLock lock = stripeLocks[stripe];
        long stamp = lock.writeLock();
        try {
            ByteBuffer keyBufDirect = lshKeyBuf.get();
            keyBufDirect.clear();
            keyBufDirect.putInt(0x4C534800).putInt(tableIdx).putInt(band);
            keyBufDirect.flip();

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer existingVal = lshDbi.get(txn, keyBufDirect);
                if (existingVal == null) return;

                long[] existing = decodeBucket(existingVal);
                int originalSize = existing.length / 2;

                long[] filtered = new long[existing.length];
                int pos = 0;
                for (int i = 0; i < existing.length; i += 2) {
                    UUID id = new UUID(existing[i], existing[i + 1]);
                    if (!id.equals(nodeId)) {
                        filtered[pos++] = existing[i];
                        filtered[pos++] = existing[i + 1];
                    }
                }

                if (pos == existing.length) return; // No change

                ByteBuffer valDirect = encodeBucket(Arrays.copyOf(filtered, pos));
                keyBufDirect.rewind();
                lshDbi.put(txn, keyBufDirect, valDirect);
                txn.commit();

                totalBucketEntries.addAndGet(-1);
            }
        } catch (Exception e) {
            log.error("LSH remove failed {}-{}", tableIdx, band, e);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private long[] decodeBucket(ByteBuffer data) {
        if (data.remaining() < 12) return new long[0];

        ByteBuffer dup = data.duplicate().order(ByteOrder.BIG_ENDIAN);
        long ts = dup.getLong(0);

        // Safe overflow-aware TTL check
        long now = System.nanoTime();
        if (now - ts > BUCKET_TTL_NS || ts > now) {
            return new long[0];
        }

        int count = dup.getInt(8);
        if (count < 0 || count > MAX_BUCKET_IDS) return new long[0];

        long[] arr = new long[count * 2];
        dup.position(12);
        dup.asLongBuffer().get(arr);
        return arr;
    }

    @Override
    public void trimBucket(int tableIdx, int band, long targetSize) {
        Set<UUID> bucket = getBucket(tableIdx, band);
        if (bucket.size() <= targetSize) return;

        List<UUID> sorted = bucket.stream()
                .sorted(Comparator.comparingLong(nodePriorityProvider::getPriority).reversed())
                .collect(Collectors.toList());

        List<UUID> keep = sorted.subList(0, (int) Math.min(targetSize, sorted.size()));
        addToBucket(tableIdx, band, keep);

        int trimmed = bucket.size() - keep.size();
        if (trimmed > 1000) {
            log.info("LSH trimmed bucket {}-{}: {} → {} (kept top {} by priority)",
                    tableIdx, band, bucket.size(), keep.size(), targetSize);
        }
    }

    @Override
    public void clearAllBuckets() {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();
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
            log.error("Clear buckets failed", e);
        }
    }

    private long[] trimByPriority(long[] pairs) {
        return IntStream.range(0, pairs.length / 2)
                .mapToObj(i -> new UUIDWithPriority(new UUID(pairs[i*2], pairs[i*2+1]),
                        nodePriorityProvider.getPriority(new UUID(pairs[i*2], pairs[i*2+1]))))
                .sorted(Comparator.comparingLong(u -> -u.priority))
                .limit(TARGET_SIZE)
                .map(u -> new long[]{u.uuid.getMostSignificantBits(), u.uuid.getLeastSignificantBits()})
                .flatMapToLong(Arrays::stream)
                .toArray();
    }

    @Override
    public void close() throws Exception {

    }

    private record UUIDWithPriority(UUID uuid, long priority) { }
}