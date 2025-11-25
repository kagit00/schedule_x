package com.shedule.x.utils.basic;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public final class Murmur3 {

    private static final int SEED = 0;

    public static int hash32(String str) {
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        // Use the 128-bit implementation and return the first 4 bytes (int)
        byte[] hash128 = hash128(data, 0, data.length, SEED);
        return ByteBuffer.wrap(hash128).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public static byte[] hash128(String str) {
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        return hash128(data, 0, data.length, SEED);
    }

    public static byte[] hash128(byte[] data) {
        return hash128(data, 0, data.length, SEED);
    }

    public static void hash128To(ByteBuffer dst, String str) {
        byte[] data = str.getBytes(StandardCharsets.UTF_8);
        hash128To(dst, data, 0, data.length, SEED);
    }

    public static void hash128To(ByteBuffer dst, byte[] data, int offset, int len) {
        hash128To(dst, data, offset, len, SEED);
    }

    public static void hash128To(ByteBuffer dst, byte[] key, int offset, int len, int seed) {
        int pos = dst.position();
        hash128(key, offset, len, seed, dst);
        dst.position(pos + 16);
    }

    public static byte[] hash128(byte[] key, int offset, int len, int seed) {
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        hash128(key, offset, len, seed, buf);
        return buf.array();
    }

    private static void hash128(byte[] key, int offset, int len, int seed, ByteBuffer out) {
        long h1 = seed;
        long h2 = seed;

        final int nblocks = len >> 4;
        for (int i = 0; i < nblocks; i++) {
            int idx = offset + (i << 4);
            long k1 = getLongLE(key, idx);
            long k2 = getLongLE(key, idx + 8);

            k1 *= 0x87c37b91114253d5L;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= 0x4cf5ad432745937fL;
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= 0x4cf5ad432745937fL;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= 0x87c37b91114253d5L;
            h2 ^= k2;
            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        long k1 = 0, k2 = 0;
        int tailIdx = offset + (nblocks << 4);
        int tail = len & 15;

        switch (tail) {
            case 15: k2 ^= (long) (key[tailIdx + 14] & 0xff) << 48;
            case 14: k2 ^= (long) (key[tailIdx + 13] & 0xff) << 40;
            case 13: k2 ^= (long) (key[tailIdx + 12] & 0xff) << 32;
            case 12: k2 ^= (long) (key[tailIdx + 11] & 0xff) << 24;
            case 11: k2 ^= (long) (key[tailIdx + 10] & 0xff) << 16;
            case 10: k2 ^= (long) (key[tailIdx + 9] & 0xff) << 8;
            case 9:  k2 ^= (key[tailIdx + 8] & 0xff);
                k2 *= 0x4cf5ad432745937fL;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= 0x87c37b91114253d5L;
                h2 ^= k2;
            case 8:  k1 ^= getLongLE(key, tailIdx);
                break;
            case 7:  k1 ^= (long) (key[tailIdx + 6] & 0xff) << 48;
            case 6:  k1 ^= (long) (key[tailIdx + 5] & 0xff) << 40;
            case 5:  k1 ^= (long) (key[tailIdx + 4] & 0xff) << 32;
            case 4:  k1 ^= (long) (key[tailIdx + 3] & 0xff) << 24;
            case 3:  k1 ^= (long) (key[tailIdx + 2] & 0xff) << 16;
            case 2:  k1 ^= (long) (key[tailIdx + 1] & 0xff) << 8;
            case 1:  k1 ^= (key[tailIdx] & 0xff);
                k1 *= 0x87c37b91114253d5L;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= 0x4cf5ad432745937fL;
                h1 ^= k1;
        }

        h1 ^= len; h2 ^= len;
        h1 += h2; h2 += h1;
        h1 = fmix(h1); h2 = fmix(h2);
        h1 += h2; h2 += h1;

        out.putLong(h1).putLong(h2);
    }

    private static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    private static long getLongLE(byte[] data, int pos) {
        return ((long)data[pos] & 0xff) |
                ((long)data[pos + 1] & 0xff) << 8 |
                ((long)data[pos + 2] & 0xff) << 16 |
                ((long)data[pos + 3] & 0xff) << 24 |
                ((long)data[pos + 4] & 0xff) << 32 |
                ((long)data[pos + 5] & 0xff) << 40 |
                ((long)data[pos + 6] & 0xff) << 48 |
                ((long)data[pos + 7] & 0xff) << 56;
    }

    public static String toHex(byte[] hash) {
        StringBuilder sb = new StringBuilder(32);
        for (byte b : hash) sb.append(String.format("%02x", b & 0xff));
        return sb.toString();
    }

    public static String toHex(byte[] array, int off, int len) {
        StringBuilder sb = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02x", array[off + i] & 0xff));
        }
        return sb.toString();
    }
}