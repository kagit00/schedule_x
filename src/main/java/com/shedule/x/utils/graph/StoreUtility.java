package com.shedule.x.utils.graph;


import com.shedule.x.utils.AlgorithmUtils;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;


@UtilityClass
@Slf4j
public final class StoreUtility {
    private static final int MAX_KEY_BUF = 128;
    private static final int MAX_BUCKET_IDS = 40_000;
    private static final int LOCK_STRIPES = 16_384;
    private static final int MAX_VAL_BUF = 512;

    private final ThreadLocal<ByteBuffer> valBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(MAX_VAL_BUF));
    private final ThreadLocal<ByteBuffer> keyBuf = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(MAX_KEY_BUF));
    private final ThreadLocal<long[]> mergeBuf = ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 4]);
    private final ThreadLocal<ByteBuffer> bucketBuf =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(MAX_BUCKET_IDS * 16 + 64));
    private final ThreadLocal<long[]> radixTmp =
            ThreadLocal.withInitial(() -> new long[MAX_BUCKET_IDS * 2]);

    public static long[] mergeAndDedupPairs(long[] existing, long[] incoming) {
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

    public static ByteBuffer encodeBucket(long[] ids) {
        ByteBuffer buf = bucketBuf.get();
        buf.clear();
        buf.putLong(System.nanoTime());
        buf.putInt(ids.length / 2);
        for (long l : ids) buf.putLong(l);
        buf.flip();
        return buf;
    }

    public static boolean matchesGroupPrefix(ByteBuffer key, UUID groupId) {
        return key.getLong(0) == groupId.getMostSignificantBits() &&
                key.getLong(8) == groupId.getLeastSignificantBits();
    }

    public static int comparePair(long aMsb, long aLsb, long bMsb, long bLsb) {
        int c = Long.compare(aMsb, bMsb);
        if (c != 0) return c;
        return Long.compare(aLsb, bLsb);
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

    public static int stripe(long hash) { return (int) ((hash >>> 16) & (LOCK_STRIPES - 1)); }
    public static ByteBuffer keyBuf() { ByteBuffer b = keyBuf.get(); b.clear(); return b; }
    public static ByteBuffer valBuf() { ByteBuffer b = valBuf.get(); b.clear(); return b; }
    public static void putUUID(ByteBuffer buf, UUID id) {
        buf.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
    }

    public static  long[] toLongArray(Collection<UUID> ids) {
        long[] arr = new long[ids.size() * 2];
        int i = 0;
        for (UUID id : ids) {
            arr[i++] = id.getMostSignificantBits();
            arr[i++] = id.getLeastSignificantBits();
        }
        return arr;
    }
}
