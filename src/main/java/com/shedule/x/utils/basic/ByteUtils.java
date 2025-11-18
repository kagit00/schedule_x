package com.shedule.x.utils.basic;

import com.shedule.x.exceptions.BadRequestException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public final class ByteUtils {
    private static final int LSH_MAGIC = 0x4C534800;

    public static long getLong(byte[] b, int offset) {
        return ((long) (b[offset]   & 0xFF) << 56) |
                ((long) (b[offset + 1] & 0xFF) << 48) |
                ((long) (b[offset + 2] & 0xFF) << 40) |
                ((long) (b[offset + 3] & 0xFF) << 32) |
                ((long) (b[offset + 4] & 0xFF) << 24) |
                ((long) (b[offset + 5] & 0xFF) << 16) |
                ((long) (b[offset + 6] & 0xFF) <<  8) |
                ((long) (b[offset + 7] & 0xFF));
    }

    public static int getInt(byte[] b, int offset) {
        return ((b[offset]   & 0xFF) << 24) |
                ((b[offset + 1] & 0xFF) << 16) |
                ((b[offset + 2] & 0xFF) <<  8) |
                ((b[offset + 3] & 0xFF));
    }

    public static int getShortUnsigned(byte[] b, int offset) {
        return ((b[offset] & 0xFF) << 8) | (b[offset + 1] & 0xFF);
    }

    public static void putLong(byte[] b, int offset, long v) {
        b[offset]     = (byte) (v >>> 56);
        b[offset + 1] = (byte) (v >>> 48);
        b[offset + 2] = (byte) (v >>> 40);
        b[offset + 3] = (byte) (v >>> 32);
        b[offset + 4] = (byte) (v >>> 24);
        b[offset + 5] = (byte) (v >>> 16);
        b[offset + 6] = (byte) (v >>>  8);
        b[offset + 7] = (byte) (v);
    }

    public static void putInt(byte[] b, int offset, int v) {
        b[offset]     = (byte) (v >>> 24);
        b[offset + 1] = (byte) (v >>> 16);
        b[offset + 2] = (byte) (v >>>  8);
        b[offset + 3] = (byte) (v);
    }

    public static void putShort(byte[] b, int offset, short v) {
        b[offset]     = (byte) ((v >>> 8) & 0xFF);
        b[offset + 1] = (byte) (v & 0xFF);
    }


}
