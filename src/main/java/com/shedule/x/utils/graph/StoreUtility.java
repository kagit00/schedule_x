package com.shedule.x.utils.graph;


import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;


@UtilityClass
@Slf4j
public final class StoreUtility {
    public static long[] mergeAndDedupPairs(long[] a, long[] b) {
        if (a.length == 0) return b.length == 0 ? new long[0] : Arrays.copyOf(b, b.length);
        if (b.length == 0) return Arrays.copyOf(a, a.length);

        int lenA = a.length / 2, lenB = b.length / 2;
        long[] result = new long[a.length + b.length];
        int i = 0, j = 0, k = 0;

        while (i < lenA && j < lenB) {
            int cmp = comparePair(a, i * 2, b, j * 2);
            if (cmp < 0) {
                result[k++] = a[i * 2];
                result[k++] = a[i * 2 + 1];
                i++;
            } else if (cmp > 0) {
                result[k++] = b[j * 2];
                result[k++] = b[j * 2 + 1];
                j++;
            } else {
                // duplicate — skip one
                result[k++] = a[i * 2];
                result[k++] = a[i * 2 + 1];
                i++; j++;
            }
        }

        while (i < lenA) {
            result[k++] = a[i * 2];
            result[k++] = a[i * 2 + 1];
            i++;
        }
        while (j < lenB) {
            result[k++] = b[j * 2];
            result[k++] = b[j * 2 + 1];
            j++;
        }

        return k == result.length ? result : Arrays.copyOf(result, k);
    }

    public static int comparePair(long[] arr, int i, long[] brr, int j) {
        long a1 = arr[i], a2 = arr[i + 1];
        long b1 = brr[j], b2 = brr[j + 1];
        int cmp = Long.compare(a1, b1);
        return cmp != 0 ? cmp : Long.compare(a2, b2);
    }

    public static boolean matchesGroupPrefix(ByteBuffer key, UUID groupId) {
        return key.getLong(0) == groupId.getMostSignificantBits() &&
                key.getLong(8) == groupId.getLeastSignificantBits();
    }
}
