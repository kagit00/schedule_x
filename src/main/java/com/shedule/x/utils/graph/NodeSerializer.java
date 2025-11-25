package com.shedule.x.utils.graph;

import com.shedule.x.dto.NodeDTO;
import com.shedule.x.models.Node;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.util.UUID;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@UtilityClass
public final class NodeSerializer {

    // Keep LMDB value buffers no larger than this.
    public static final int MAX_NODE_SIZE = 512;

    /**
     * Serializes NodeDTO into the provided ByteBuffer.
     * The method ensures the written data does not exceed MAX_NODE_SIZE.
     * The caller must call buf.flip() after this method returns.
     */
    public static void serialize(NodeDTO node, ByteBuffer buf) {
        buf.clear();
        final int startPos = buf.position();

        // -------------------- FIXED FIELDS --------------------
        putUUID(buf, node.getId());
        putUUID(buf, node.getGroupId());
        putUUID(buf, node.getDomainId());

        long epochMillis = (node.getCreatedAt() != null)
                ? node.getCreatedAt().toInstant(ZoneOffset.UTC).toEpochMilli()
                : 0L;
        buf.putLong(epochMillis);

        buf.put(node.isProcessed() ? (byte) 1 : (byte) 0);

        // -------------------- VARIABLE FIELDS --------------------
        if (!safeWriteString(buf, node.getType())) {
            finalizeAndClamp(buf, startPos);
            return;
        }

        if (!safeWriteString(buf, node.getReferenceId())) {
            finalizeAndClamp(buf, startPos);
            return;
        }

        // -------------------- METADATA --------------------
        int metaCountPos = buf.position();
        buf.putShort((short) 0); // placeholder
        int metaWritten = 0;

        if (node.getMetaData() != null && !node.getMetaData().isEmpty()) {
            for (Map.Entry<String, String> entry : node.getMetaData().entrySet()) {

                int before = buf.position();

                // Attempt to write key + value safely
                if (!safeWriteString(buf, entry.getKey()) ||
                        !safeWriteString(buf, entry.getValue())) {

                    // rollback and stop metadata writing
                    buf.position(before);
                    break;
                }

                metaWritten++;
            }
        }

        // Patch metadata count
        int current = buf.position();
        buf.position(metaCountPos);
        buf.putShort((short) metaWritten);
        buf.position(current);

        // -------------------- FINAL CLAMP --------------------
        finalizeAndClamp(buf, startPos);
    }

    private static void finalizeAndClamp(ByteBuffer buf, int startPos) {
        int written = buf.position() - startPos;
        int limit = Math.min(startPos + MAX_NODE_SIZE, startPos + written);
        buf.position(limit);
    }



    /**
     * Deserialize buffer into NodeDTO.
     * Expects ByteBuffer position to be at the start of the value.
     */
    public static NodeDTO deserialize(ByteBuffer val) {
        if (val == null || val.remaining() < 16) return null;

        int startPos = val.position();
        int avail = Math.min(val.remaining(), MAX_NODE_SIZE);

        try {
            UUID id = getUUID(val);
            UUID groupId = getUUID(val);
            UUID domainId = getUUID(val);

            long epochMillis = val.getLong();
            LocalDateTime createdAt = (epochMillis == 0L)
                    ? null
                    : LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);

            boolean processed = val.get() == (byte)1;

            String type = readShortPrefixedString(val);
            String refId = readShortPrefixedString(val);

            int metaCount = Short.toUnsignedInt(val.getShort());
            Map<String, String> meta = new java.util.HashMap<>(Math.max(4, metaCount));

            for (int i = 0; i < metaCount; i++) {
                // Defensive check against buffer limit
                if (val.position() - startPos >= avail) break;

                String k = readShortPrefixedString(val);
                String v = readShortPrefixedString(val);
                if (k == null) break;
                meta.put(k, v);
            }

            return NodeDTO.builder()
                    .id(id)
                    .groupId(groupId)
                    .domainId(domainId)
                    .createdAt(createdAt)
                    .processed(processed)
                    .type(type)
                    .referenceId(refId)
                    .metaData(meta)
                    .build();
        } catch (Exception ex) {
            log.warn("Failed to deserialize NodeDTO from buffer at position {}", startPos, ex);
            return null;
        }
    }





    private static void putUUID(ByteBuffer buf, UUID id) {
        if (id == null) {
            buf.putLong(0L);
            buf.putLong(0L);
        } else {
            buf.putLong(id.getMostSignificantBits());
            buf.putLong(id.getLeastSignificantBits());
        }
    }

    private static UUID getUUID(ByteBuffer buf) {
        long msb = buf.getLong();
        long lsb = buf.getLong();
        if (msb == 0L && lsb == 0L) return null;
        return new UUID(msb, lsb);
    }

    private static boolean safeWriteString(ByteBuffer buf, String s) {
        if (s == null) s = "";

        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int len = Math.min(bytes.length, Short.MAX_VALUE);

        int required = 2 + len; // 2 bytes length prefix + content

        if (buf.remaining() < required)
            return false;

        buf.putShort((short) len);
        buf.put(bytes, 0, len);
        return true;
    }


    private static String readShortPrefixedString(ByteBuffer buf) {
        int len = Short.toUnsignedInt(buf.getShort());
        if (len == 0) return "";
        if (len > buf.remaining()) {
            // malformed — clamp
            len = buf.remaining();
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}