package com.shedule.x.processors;

import com.shedule.x.utils.basic.Murmur3;
import com.shedule.x.utils.graph.StoreUtility;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;

@Component
public class DefaultKeyPrefixProvider implements KeyPrefixProvider {

    private static final ThreadLocal<ByteBuffer> TEMP = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(64).order(ByteOrder.BIG_ENDIAN));
    private static final ThreadLocal<ByteBuffer> PREFIX = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(32).order(ByteOrder.BIG_ENDIAN));

    @Override
    public ByteBuffer makePrefix(UUID groupId, String cycleId) {
        ByteBuffer bb = PREFIX.get(); bb.clear();
        StoreUtility.putUUID(bb, groupId);
        Murmur3.hash128To(bb, cycleId);
        bb.flip();
        return bb;
    }

    @Override
    public ByteBuffer makePrefix(UUID groupId) {
        ByteBuffer bb = ByteBuffer.allocateDirect(16).order(ByteOrder.BIG_ENDIAN);
        StoreUtility.putUUID(bb, groupId);
        bb.flip();
        return bb;
    }

    @Override
    public ByteBuffer makePrefix(Object... parts) {
        ByteBuffer bb = TEMP.get(); bb.clear();
        for (Object p : parts) {
            if (p instanceof UUID u) StoreUtility.putUUID(bb, u);
            else if (p instanceof String s) Murmur3.hash128To(bb, s);
            else throw new IllegalArgumentException("Unsupported part: " + p);
        }
        bb.flip();
        return bb;
    }

    @Override
    public boolean matchesPrefix(ByteBuffer key, Object... expected) {
        if (key.remaining() < expected.length * 16) return false;
        int pos = 0;
        for (Object p : expected) {
            if (p instanceof UUID u) {
                if (key.getLong(pos) != u.getMostSignificantBits() ||
                    key.getLong(pos + 8) != u.getLeastSignificantBits()) return false;
                pos += 16;
            } else if (p instanceof String s) {
                byte[] hash = Murmur3.hash128(s);
                for (int i = 0; i < 16; i++) {
                    if (key.get(pos + i) != hash[i]) return false;
                }
                pos += 16;
            }
        }
        return true;
    }
}