package com.shedule.x.utils.basic;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public final class ByteUtils {
    public static long getLong(byte[] b, int offset) {
        return ((long)(b[offset]   & 255) << 56) |
                ((long)(b[offset+1] & 255) << 48) |
                ((long)(b[offset+2] & 255) << 40) |
                ((long)(b[offset+3] & 255) << 32) |
                ((long)(b[offset+4] & 255) << 24) |
                ((long)(b[offset+5] & 255) << 16) |
                ((long)(b[offset+6] & 255) << 8)  |
                ((long)(b[offset+7] & 255));
    }

    public static int getInt(byte[] b, int offset) {
        return ((b[offset]   & 255) << 24) |
                ((b[offset+1] & 255) << 16) |
                ((b[offset+2] & 255) << 8)  |
                ((b[offset+3] & 255));
    }

    public static int getShortUnsigned(byte[] b, int offset) {
        return ((b[offset] & 255) << 8) | (b[offset+1] & 255);
    }

    public static void putLong(byte[] b, int offset, long v) {
        b[offset]   = (byte)(v >> 56);
        b[offset+1] = (byte)(v >> 48);
        b[offset+2] = (byte)(v >> 40);
        b[offset+3] = (byte)(v >> 32);
        b[offset+4] = (byte)(v >> 24);
        b[offset+5] = (byte)(v >> 16);
        b[offset+6] = (byte)(v >> 8);
        b[offset+7] = (byte)(v);
    }

    public static void putInt(byte[] b, int offset, int v) {
        b[offset]   = (byte)(v >> 24);
        b[offset+1] = (byte)(v >> 16);
        b[offset+2] = (byte)(v >> 8);
        b[offset+3] = (byte)(v);
    }

    public static void putShort(byte[] b, int offset, short v) {
        b[offset]   = (byte)((v >> 8) & 0xFF);
        b[offset+1] = (byte)(v & 0xFF);
    }

    public byte[] getLSHKey(int tableIdx, int band) {
        final int LSH_MAGIC = 0x4C534800; // "LSH\0"
        final int MAX_TABLE_IDX = 65535;
        final int MAX_BAND = 65535;

        if (tableIdx < 0 || tableIdx > MAX_TABLE_IDX || band < 0 || band > MAX_BAND) {
            throw new IllegalArgumentException("Invalid LSH bucket: table=" + tableIdx + ", band=" + band);
        }
        byte[] key = new byte[8];
        putInt(key, 0, LSH_MAGIC);
        putShort(key, 4, (short) tableIdx);
        putShort(key, 6, (short) band);
        return key;
    }
}
