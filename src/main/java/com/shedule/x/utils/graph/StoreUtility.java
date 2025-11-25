package com.shedule.x.utils.graph;


import com.shedule.x.processors.LshBucketManagerImpl;
import com.shedule.x.utils.AlgorithmUtils;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.IntStream;

@UtilityClass
@Slf4j
public final class StoreUtility {
    private static final int LOCK_STRIPES = 65_536;
    private static final int STRIPE_MASK = LOCK_STRIPES - 1;
    private static final long BUCKET_TTL_MS = 24L * 60 * 60 * 1000;
    private static final int MAX_KEY_BUF = 128;
    private static final int MAX_BUCKET_IDS = 40_000;
    private static final int MAX_VAL_BUF = 512;

    private final ThreadLocal<ByteBuffer> valBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(MAX_VAL_BUF).order(ByteOrder.BIG_ENDIAN));

    private final ThreadLocal<ByteBuffer> keyBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(MAX_KEY_BUF).order(ByteOrder.BIG_ENDIAN));

    private final ThreadLocal<long[]> mergeBuf = ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 4]);

    private final ThreadLocal<ByteBuffer> bucketBuf =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(MAX_BUCKET_IDS * 16 + 64).order(ByteOrder.BIG_ENDIAN));

    private final ThreadLocal<long[]> radixTmp =
            ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 2]);


    public static int stripe(long hash) {

        int mixed = Long.hashCode(hash);

        mixed = (mixed ^ 61) ^ (mixed >>> 16);
        mixed = mixed + (mixed << 3);
        mixed = mixed ^ (mixed >>> 4);
        mixed = mixed * 0x27d4eb2d;
        mixed = mixed ^ (mixed >>> 15);

        return mixed & STRIPE_MASK;
    }

    public static long[] mergeAndDedupPairs(long[] existing, long[] incoming) {
        if ((existing.length & 1) != 0 || (incoming.length & 1) != 0)
            throw new IllegalArgumentException("pair arrays must have even length");

        int n1 = existing.length;
        int n2 = incoming.length;
        long[] tmp = mergeBuf.get();

        if (tmp.length < n1 + n2) {
            tmp = new long[n1 + n2 + 1024];
            mergeBuf.set(tmp);
        }

        int i = 0, j = 0, w = 0;

        while (i < n1 && j < n2) {
            long aMsb = existing[i];
            long bMsb = incoming[j];

            // Compare MSB first
            int cmp = Long.compare(aMsb, bMsb);
            if (cmp == 0) {
                // If MSB matches, compare LSB
                cmp = Long.compare(existing[i + 1], incoming[j + 1]);
            }

            if (cmp < 0) {
                tmp[w++] = existing[i++]; tmp[w++] = existing[i++];
            } else if (cmp > 0) {
                tmp[w++] = incoming[j++]; tmp[w++] = incoming[j++];
            } else {
                tmp[w++] = existing[i++]; tmp[w++] = existing[i++];
                j += 2;
            }
        }

        while (i < n1) {
            tmp[w++] = existing[i++]; tmp[w++] = existing[i++];
        }
        while (j < n2) {
            tmp[w++] = incoming[j++]; tmp[w++] = incoming[j++];
        }

        return Arrays.copyOf(tmp, w);
    }

    public static ByteBuffer encodeBucket(long[] ids) {
        ByteBuffer buf = bucketBuf.get();
        int required = ids.length * 8 + 64;
        if (buf.capacity() < required) {
            buf = ByteBuffer.allocateDirect(required * 2).order(ByteOrder.BIG_ENDIAN);
            bucketBuf.set(buf);
        }

        buf.clear();
        buf.putLong(System.currentTimeMillis());
        buf.putInt(ids.length / 2);
        for (long l : ids) buf.putLong(l);
        buf.flip();
        return buf;
    }

    public static boolean matchesGroupPrefix(ByteBuffer key, UUID groupId) {
        if (key.remaining() < 16) return false;
        return key.getLong(0) == groupId.getMostSignificantBits() &&
                key.getLong(8) == groupId.getLeastSignificantBits();
    }

    public static int comparePair(long aMsb, long aLsb, long bMsb, long bLsb) {
        int c = Long.compare(aMsb, bMsb);
        return (c != 0) ? c : Long.compare(aLsb, bLsb);
    }

    public static void sortByPairs(long[] pairs) {
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

    public static ByteBuffer keyBuf() { ByteBuffer b = keyBuf.get(); b.clear(); return b; }
    public static ByteBuffer valBuf() { ByteBuffer b = valBuf.get(); b.clear(); return b; }

    public static void putUUID(ByteBuffer buf, UUID id) {
        buf.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
    }

    public static long[] toLongArray(Collection<UUID> ids) {
        long[] arr = new long[ids.size() * 2];
        int i = 0;
        for (UUID id : ids) {
            arr[i++] = id.getMostSignificantBits();
            arr[i++] = id.getLeastSignificantBits();
        }
        return arr;
    }

    public static long[] decodeBucket(ByteBuffer data) {
        if (data.remaining() < 12) return new long[0];

        long ts = data.getLong(0);

        if (isExpired(ts)) {
            return new long[0];
        }

        int count = data.getInt(8);
        if (count <= 0 || count > MAX_BUCKET_IDS) return new long[0];

        if (data.remaining() < 12 + (count * 16)) return new long[0];

        long[] arr = new long[count * 2];
        data.position(12);
        data.asLongBuffer().get(arr);
        return arr;
    }

    public static boolean isExpired(long ts) {
        long now = System.currentTimeMillis();
        return (now - ts > BUCKET_TTL_MS) || (ts > now + 60000);
    }

    public static boolean keyStartsWith(ByteBuffer key, ByteBuffer prefix) {
        ByteBuffer k = key.duplicate();
        ByteBuffer p = prefix.duplicate();
        int oldKPos = k.position();
        int oldPPos = p.position();
        try {
            int pRem = p.remaining();
            if (k.remaining() < pRem) return false;
            for (int i = 0; i < pRem; i++) {
                if (k.get() != p.get()) return false;
            }
            return true;
        } finally {
            k.position(oldKPos);
            p.position(oldPPos);
        }
    }

    public static UUID readUUIDFromBuffer(ByteBuffer buf) {
        long msb = buf.getLong();
        long lsb = buf.getLong();
        if (msb == 0L && lsb == 0L) return null;
        return new UUID(msb, lsb);
    }
}