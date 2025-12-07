package com.shedule.x.utils.graph;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;



public final class EdgeKeyBuilder {

    private static final ThreadLocal<ByteBuffer> BUF =
            ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(128).order(ByteOrder.BIG_ENDIAN));

    private static final ThreadLocal<MessageDigest> SHA256 =
            ThreadLocal.withInitial(() -> {
                try { return MessageDigest.getInstance("SHA-256"); }
                catch (Exception e) { throw new RuntimeException(e); }
            });

    private EdgeKeyBuilder() {}

    public static ByteBuffer build(UUID groupId, String cycleId, String a, String b) {
        if (a.compareTo(b) > 0) {
            String t = a; a = b; b = t;
        }

        String pair = a + "\0" + b;

        ByteBuffer buf = BUF.get();
        buf.clear();

        buf.putLong(groupId.getMostSignificantBits());
        buf.putLong(groupId.getLeastSignificantBits());

        MessageDigest md = SHA256.get();

        md.reset();
        buf.put(md.digest(cycleId.getBytes(StandardCharsets.UTF_8)));

        md.reset();
        buf.put(md.digest(pair.getBytes(StandardCharsets.UTF_8)));

        buf.flip();
        return buf;
    }
}
