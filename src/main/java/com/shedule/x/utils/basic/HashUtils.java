package com.shedule.x.utils.basic;


import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.charset.StandardCharsets;
import java.util.Random;


@ThreadSafe
public final class HashUtils {

    private static final int MAX_SEEDS = 1000;
    private static final short MAX_HASH = (short) 0xFFFF; // 65535

    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32_fixed();

    private static final long[] SEEDS = new long[MAX_SEEDS];

    private static final ThreadLocal<int[]> ROW_HASHES = ThreadLocal.withInitial(
            () -> new int[32]
    );

    static {
        Random r = new Random(42);
        for (int i = 0; i < SEEDS.length; i++) {
            SEEDS[i] = r.nextLong();
        }
    }


    public static short[] computeHashes(int[] features, int numHashTables, int numBands) {
        if (numBands <= 0 || numHashTables <= 0) {
            throw new IllegalArgumentException("numBands and numHashTables must be > 0");
        }
        if (numHashTables % numBands != 0) {
            throw new IllegalArgumentException("numHashTables must be divisible by numBands");
        }

        final int rowsPerBand = numHashTables / numBands;
        short[] bandHashes = new short[numBands];
        int[] rowHashes = ROW_HASHES.get();

        if (rowHashes.length < rowsPerBand) {
            rowHashes = new int[rowsPerBand];
        }

        int tableIndex = 0;
        for (int band = 0; band < numBands; band++) {
            for (int r = 0; r < rowsPerBand; r++) {
                rowHashes[r] = getMinHash(features, tableIndex++);
            }

            int combined = 0;
            for (int i = 0; i < rowsPerBand; i++) {
                combined ^= rowHashes[i];
                combined *= 0x9e3779b9; // Golden ratio mix
            }

            bandHashes[band] = (short) (combined & MAX_HASH);
        }

        return bandHashes;
    }


    private static int getMinHash(int[] features, int tableIndex) {
        long seed = SEEDS[tableIndex % MAX_SEEDS];
        int minHash = Integer.MAX_VALUE;

        if (features == null || features.length == 0) {
            return murmur3Int((int) seed) & 0x7FFFFFFF;
        }

        for (int feature : features) {
            int h = murmur3Int(feature ^ (int)(seed >>> 32), (int)seed);
            if (h < minHash) {
                minHash = h;
            }
        }

        return minHash & 0x7FFFFFFF;
    }

    private static int murmur3Int(int value) {
        return HASH_FUNCTION.hashInt(value).asInt();
    }

    private static int murmur3Int(int value, int seed) {
        return HASH_FUNCTION.newHasher().putInt(value).putInt(seed).hash().asInt();
    }

    public static int hashString(String str) {
        return HASH_FUNCTION.hashString(str, StandardCharsets.UTF_8).asInt();
    }

    private HashUtils() {}
}