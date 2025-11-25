package com.shedule.x.processors;

import com.shedule.x.config.factory.NodePriorityProvider;
import com.shedule.x.utils.graph.StoreUtility;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import java.util.stream.IntStream;
import static com.shedule.x.utils.graph.StoreUtility.*;
import java.util.*;


@Slf4j
@Component
public class LshBucketManagerImpl implements LshBucketManager {
    private static final int LOCK_STRIPES = 65_536;
    private static final int LSH_BUCKET_TARGET_SIZE = 2_000;
    private final AtomicLong totalBucketEntries = new AtomicLong(0);

    @Autowired
    private NodePriorityProvider nodePriorityProvider;

    @Autowired
    private LmdbEnvironment lmdb;

    private final StampedLock[] stripeLocks = new StampedLock[LOCK_STRIPES];

    @PostConstruct
    void init() {
        for (int i = 0; i < LOCK_STRIPES; i++) {
            stripeLocks[i] = new StampedLock();
        }
        log.info("LshBucketManager initialized with {} lock stripes", LOCK_STRIPES);
    }

    @Override
    public Set<UUID> getBucket(int tableIdx, int band) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();

        ByteBuffer key = StoreUtility.keyBuf();
        key.putInt(0x4C534800).putInt(tableIdx).putInt(band);
        key.flip();

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer val = lshDbi.get(txn, key);
            if (val == null) return Set.of();

            long[] raw = decodeBucket(val);
            if (raw.length == 0) return Set.of();

            Set<UUID> set = new HashSet<>(raw.length / 2);
            for (int i = 0; i < raw.length; i += 2) {
                set.add(new UUID(raw[i], raw[i + 1]));
            }
            return set;
        } catch (Exception e) {
            log.warn("LSH get fail {}-{}", tableIdx, band, e);
            return Set.of();
        }
    }

    @Override
    public int getBucketSize(int tableIdx, int band) {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();

        ByteBuffer key = StoreUtility.keyBuf();
        key.putInt(0x4C534800).putInt(tableIdx).putInt(band);
        key.flip();

        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer val = lshDbi.get(txn, key);
            if (val == null || val.remaining() < 12) return 0;

            long ts = val.getLong(0);
            if (isExpired(ts)) return 0;

            return val.getInt(8);
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public void addToBucket(int tableIdx, int band, Collection<UUID> nodeIds) {
        if (nodeIds.isEmpty()) return;

        // 1. Determine Lock Stripe using Utility
        long hash = ((long) tableIdx << 32) | (band & 0xFFFFFFFFL);
        int stripe = StoreUtility.stripe(hash);

        StampedLock lock = stripeLocks[stripe];
        long stamp = lock.writeLock();

        try {
            Env<ByteBuffer> env = lmdb.env();
            Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();

            // 2. Prepare Key
            ByteBuffer keyBuf = StoreUtility.keyBuf();
            keyBuf.putInt(0x4C534800).putInt(tableIdx).putInt(band);
            keyBuf.flip();

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer existingVal = lshDbi.get(txn, keyBuf);

                // 3. Decode Existing
                long[] existing = (existingVal == null) ? new long[0] : decodeBucket(existingVal);

                // 4. Prepare Incoming: Convert -> Sort
                long[] incoming = StoreUtility.toLongArray(nodeIds);
                StoreUtility.sortByPairs(incoming);

                // 5. Fast Primitive Merge
                long[] merged = StoreUtility.mergeAndDedupPairs(existing, incoming);
                int finalSize = merged.length / 2;

                // 6. Trim if needed
                if (finalSize > LSH_BUCKET_TARGET_SIZE) {
                    merged = trimByPriority(merged);
                    finalSize = merged.length / 2;
                }

                // 7. Encode using Utility
                ByteBuffer valDirect = StoreUtility.encodeBucket(merged);

                // 8. Write
                keyBuf.rewind();
                lshDbi.put(txn, keyBuf, valDirect);
                txn.commit();

                // Metrics
                long added = finalSize - (existing.length / 2);
                if (added != 0) {
                    totalBucketEntries.addAndGet(added);
                }
            }
        } catch (Exception e) {
            log.error("LSH add failed {}-{}", tableIdx, band, e);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void removeFromBucket(int tableIdx, int band, UUID nodeId) {
        long hash = ((long) tableIdx << 32) | (band & 0xFFFFFFFFL);
        int stripe = StoreUtility.stripe(hash);
        StampedLock lock = stripeLocks[stripe];
        long stamp = lock.writeLock();

        try {
            Env<ByteBuffer> env = lmdb.env();
            Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();

            ByteBuffer keyBuf = StoreUtility.keyBuf();
            keyBuf.putInt(0x4C534800).putInt(tableIdx).putInt(band);
            keyBuf.flip();

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer existingVal = lshDbi.get(txn, keyBuf);
                if (existingVal == null) return;

                long[] existing = decodeBucket(existingVal);
                if (existing.length == 0) return;

                // Filter logic
                long targetMsb = nodeId.getMostSignificantBits();
                long targetLsb = nodeId.getLeastSignificantBits();

                // Check if removal is needed (linear scan is fine for bucket sizes)
                boolean found = false;
                int count = existing.length / 2;
                for (int i=0; i<count; i++) {
                    if (existing[i*2] == targetMsb && existing[i*2+1] == targetLsb) {
                        found = true;
                        break;
                    }
                }

                if (!found) return;

                // Rebuild array without the target
                long[] filtered = new long[existing.length - 2];
                int pos = 0;
                for (int i = 0; i < existing.length; i += 2) {
                    long msb = existing[i];
                    long lsb = existing[i+1];
                    if (msb == targetMsb && lsb == targetLsb) continue;
                    filtered[pos++] = msb;
                    filtered[pos++] = lsb;
                }

                ByteBuffer valDirect = StoreUtility.encodeBucket(filtered);
                keyBuf.rewind();
                lshDbi.put(txn, keyBuf, valDirect);
                txn.commit();

                totalBucketEntries.decrementAndGet();
            }
        } catch (Exception e) {
            log.error("LSH remove failed {}-{}", tableIdx, band, e);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void trimBucket(int tableIdx, int band, long targetSize) {
        long hash = ((long) tableIdx << 32) | (band & 0xFFFFFFFFL);
        int stripe = StoreUtility.stripe(hash);
        StampedLock lock = stripeLocks[stripe];
        long stamp = lock.writeLock();

        try {
            Env<ByteBuffer> env = lmdb.env();
            Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();
            ByteBuffer keyBuf = StoreUtility.keyBuf();
            keyBuf.putInt(0x4C534800).putInt(tableIdx).putInt(band);
            keyBuf.flip();

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                ByteBuffer existingVal = lshDbi.get(txn, keyBuf);
                if (existingVal == null) return;

                long[] existing = decodeBucket(existingVal);
                if (existing.length / 2 <= targetSize) return;

                long[] trimmed = trimByPriority(existing);

                ByteBuffer valDirect = StoreUtility.encodeBucket(trimmed);
                keyBuf.rewind();
                lshDbi.put(txn, keyBuf, valDirect);
                txn.commit();
            }
        } catch (Exception e) {
            log.error("LSH trim failed", e);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void clearAllBuckets() {
        Env<ByteBuffer> env = lmdb.env();
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi();
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            lshDbi.drop(txn); // Efficiently drop the DB content
            txn.commit();
            totalBucketEntries.set(0);
            log.info("Cleared all LSH buckets");
        } catch (Exception e) {
            log.error("Clear buckets failed", e);
        }
    }


    private long[] trimByPriority(long[] pairs) {
        return IntStream.range(0, pairs.length / 2)
                .mapToObj(i -> {
                    long msb = pairs[i * 2];
                    long lsb = pairs[i * 2 + 1];
                    UUID uuid = new UUID(msb, lsb);
                    long prio = nodePriorityProvider.getPriority(uuid);
                    return new UUIDWithPriority(msb, lsb, prio);
                })
                .sorted(Comparator.comparingLong(UUIDWithPriority::priority).reversed())
                .limit(LSH_BUCKET_TARGET_SIZE)
                .map(u -> new long[]{u.msb, u.lsb})
                .flatMapToLong(Arrays::stream)
                .toArray();
    }

    @Override
    public void mergeAndWriteBucket(Txn<ByteBuffer> txn, int tableIdx, int band, List<UUID> newIds) {

        // MAX_BUCKET_SIZE_HARD should be defined in LshBucketManagerImpl
        final int MAX_BUCKET_SIZE_HARD = 5_000;
        Dbi<ByteBuffer> lshDbi = lmdb.lshDbi(); // Assuming lmdb is a field

        // 1. Prepare Key
        ByteBuffer keyBuf = StoreUtility.keyBuf();
        keyBuf.putInt(0x4C534800).putInt(tableIdx).putInt(band).flip();

        // 2. Read Existing (using the shared TXN)
        ByteBuffer existingVal = lshDbi.get(txn, keyBuf);
        long[] existing = (existingVal == null) ? new long[0] : decodeBucket(existingVal); // decodeBucket is an existing helper

        // 3. Prepare Incoming
        long[] incoming = StoreUtility.toLongArray(newIds); // Existing utility
        StoreUtility.sortByPairs(incoming); // Existing utility

        // 4. Merge & Dedup (CPU intensive but unavoidable for packed storage)
        long[] merged = StoreUtility.mergeAndDedupPairs(existing, incoming);

        // 5. Fast Trim (Size only - NO slow priority provider calls)
        if (merged.length / 2 > MAX_BUCKET_SIZE_HARD) {
            merged = Arrays.copyOf(merged, MAX_BUCKET_SIZE_HARD * 2);
        }

        // 6. Write Back (using the shared TXN)
        ByteBuffer valDirect = StoreUtility.encodeBucket(merged); // Existing utility
        keyBuf.rewind();
        lshDbi.put(txn, keyBuf, valDirect);

        // IMPORTANT: NO Java lock, NO txn.commit().
    }

    @Override
    public void close() throws Exception {
        // No op, LMDB handles closing
    }

    private record UUIDWithPriority(long msb, long lsb, long priority) { }
}