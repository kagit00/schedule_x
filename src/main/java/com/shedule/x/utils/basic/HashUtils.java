package com.shedule.x.utils.basic;

import lombok.experimental.UtilityClass;

@UtilityClass
public final class HashUtils {
    private static final long M = 0xc6a4a7935bd1e995L;
    private static final int R = 47;

    public static short[] computeHashes(int[] metadata, int numHashTables, int numBands) {
        if (metadata == null || metadata.length == 0) {
            throw new IllegalArgumentException("Invalid metadata array");
        }
        short[] hashes = new short[numHashTables];
        for (int i = 0; i < numHashTables; i++) {
            hashes[i] = (short) computeHash(metadata, i, numBands);
        }
        return hashes;
    }

    public static int computeHash(int[] metadata, int tableIndex, int numBands) {
        long seed = tableIndex * 31L;
        int hash = 0;
        for (int i = 0; i < metadata.length; i++) {
            hash ^= fastHash(metadata[i], seed + i);
        }
        return hash & (numBands - 1);
    }

    private static int fastHash(int data, long seed) {
        long h = seed ^ (data * M);
        h ^= h >>> R;
        h *= M;
        h ^= h >>> R;
        return (int) (h & 0x7FFFFFFF);
    }
}