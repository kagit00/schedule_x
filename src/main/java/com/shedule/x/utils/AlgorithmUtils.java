package com.shedule.x.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
@UtilityClass
public final class AlgorithmUtils {
    public static void radixSortPairs(long[] pairs, long[] tmp) {
        int n = pairs.length;
        if ((n & 1) != 0) throw new IllegalArgumentException("pairs length must be even");
        if (n == 0) return;

        radixSort64OnPairIndex(pairs, tmp, 1);
        radixSort64OnPairIndex(pairs, tmp, 0);
    }

    private static void radixSort64OnPairIndex(long[] src, long[] tmp, int indexOfPair) {
        final int bytes = 8;
        final int RADIX = 256;
        final int nPairs = src.length / 2;

        long[] in = src;
        long[] out = tmp;
        boolean inSrc = true;

        int[] count = new int[RADIX];

        for (int pass = 0; pass < bytes; pass++) {
            Arrays.fill(count, 0);

            // counting
            int base = indexOfPair; // 0 or 1
            for (int i = 0; i < nPairs; i++) {
                long v = in[i*2 + base];
                int b = (int) ((v >>> (pass * 8)) & 0xFF);
                count[b]++;
            }

            // prefix sum
            int sum = 0;
            for (int i = 0; i < RADIX; i++) {
                int c = count[i];
                count[i] = sum;
                sum += c;
            }

            // distribute pairs
            for (int i = 0; i < nPairs; i++) {
                int pos = count[(int) ((in[i*2 + base] >>> (pass * 8)) & 0xFF)]++;
                out[pos*2] = in[i*2];
                out[pos*2 + 1] = in[i*2 + 1];
            }

            // swap
            long[] swap = in; in = out; out = swap;
            inSrc = !inSrc;
        }

        if (!inSrc) {
            System.arraycopy(in, 0, src, 0, src.length);
        }
    }

}
