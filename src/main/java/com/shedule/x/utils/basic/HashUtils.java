package com.shedule.x.utils.basic;

import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;


@UtilityClass
public final class HashUtils {
    private static final long M = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    private static final long[] SEEDS;
    private static final int MAX_HASH = 0xFFFF;

    static {
        SEEDS = new long[200];
        Random r = new Random(42);
        for (int i = 0; i < SEEDS.length; i++) {
            SEEDS[i] = r.nextLong();
        }
    }


    private static int computeHashForSlice(int[] metadata, int start, int length, int tableIndex) {
        long seed = tableIndex * 0x9e3779b97f4a7c15L;
        int hash = 0;

        for (int j = 0; j < length; j++) {
            int idx = (start + j) % metadata.length;
            int element = metadata[idx];

            hash ^= fastHash(element, seed);
            seed = Long.rotateLeft(seed, 5);
        }

        return hash & 0xFFFF;
    }

    private static int fastHash(int data, long seed) {
        long h = seed ^ (data * M);
        h ^= h >>> R;
        h *= M;
        h ^= h >>> R;
        return (int) (h & 0x7FFFFFFF);
    }

    public static short[] computeHashes(int[] features, int numTables, int numBands) {
        short[] minHashes = new short[numTables];
        Arrays.fill(minHashes, (short)MAX_HASH);

        for (int feature : features) {
            for (int i = 0; i < numTables; i++) {
                long seed = SEEDS[i];
                int hash = murmur3_32(feature, seed);
                int bucket = hash & MAX_HASH;
                if (bucket < (minHashes[i] & 0xFFFF)) {
                    minHashes[i] = (short) bucket;
                }
            }
        }

        return minHashes;
    }

    private static int murmur3_32(int key, long seed) {
        long h = seed ^ Integer.BYTES;
        h ^= key;
        h *= 0x85ebca6bL;
        h ^= h >>> 13;
        h *= 0xc2b2ae35L;
        h ^= h >>> 16;
        return (int) h;
    }
}