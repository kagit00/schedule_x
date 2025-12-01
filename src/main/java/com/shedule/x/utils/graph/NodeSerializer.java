package com.shedule.x.utils.graph;

import com.shedule.x.dto.NodeDTO;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import static com.shedule.x.utils.graph.StoreUtility.getUUID;
import static com.shedule.x.utils.graph.StoreUtility.putUUID;


@Slf4j
@UtilityClass
public final class NodeSerializer {
    public static final int MAX_NODE_SIZE = 2048;

    public static void serialize(NodeDTO node, ByteBuffer buf) {
        buf.clear();
        final int startPos = buf.position();

        putUUID(buf, node.getId());
        putUUID(buf, node.getGroupId());
        putUUID(buf, node.getDomainId());

        long epochMillis = (node.getCreatedAt() != null)
                ? node.getCreatedAt().toInstant(ZoneOffset.UTC).toEpochMilli()
                : 0L;
        buf.putLong(epochMillis);

        buf.put(node.isProcessed() ? (byte) 1 : (byte) 0);

        if (!safeWriteString(buf, node.getType())) {
            finalizeAndClamp(buf, startPos);
            return;
        }

        if (!safeWriteString(buf, node.getReferenceId())) {
            finalizeAndClamp(buf, startPos);
            return;
        }

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

        int current = buf.position();
        buf.position(metaCountPos);
        buf.putShort((short) metaWritten);
        buf.position(current);

        if (buf.position() > startPos + MAX_NODE_SIZE) {
            buf.position(startPos + MAX_NODE_SIZE);
        }
        buf.flip();
    }

    private static void finalizeAndClamp(ByteBuffer buf, int startPos) {
        int written = buf.position() - startPos;
        int limit = Math.min(startPos + MAX_NODE_SIZE, startPos + written);
        buf.position(limit);
    }

    public static NodeDTO deserialize(ByteBuffer val) {
        if (val == null || val.remaining() < 48) return null;

        int startPos = val.position();
        int limit = val.limit();

        try {
            UUID id = getUUID(val);
            UUID groupId = getUUID(val);
            UUID domainId = getUUID(val);

            long epochMillis = val.getLong();
            LocalDateTime createdAt = (epochMillis == 0L)
                    ? null
                    : LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);

            boolean processed = val.get() == (byte)1;

            String type = safeReadString(val);
            String refId = safeReadString(val);

            // Read metadata count safely
            Map<String, String> meta = new HashMap<>();
            if (val.remaining() >= 2) {
                int metaCount = Short.toUnsignedInt(val.getShort());
                for (int i = 0; i < metaCount && val.remaining() >= 2; i++) {
                    String key = safeReadString(val);
                    if (key == null || val.remaining() < 2) break;
                    String value = safeReadString(val);
                    if (value != null) {
                        meta.put(key, value);
                    }
                }
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
            log.warn("Failed to deserialize NodeDTO from buffer (pos={}, remaining={})", startPos, val.remaining(), ex);
            val.position(startPos); // don't corrupt stream
            return null;
        }
    }

    private static String safeReadString(ByteBuffer buf) {
        if (buf.remaining() < 2) return null;
        int len = Short.toUnsignedInt(buf.getShort());
        if (len == 0) return "";
        if (len > buf.remaining()) {
            log.debug("Truncated string in node data: expected {} bytes, only {} left", len, buf.remaining());
            return null; // or return partial? better to skip
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static boolean safeWriteString(ByteBuffer buf, String s) {
        if (s == null) s = "";

        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int len = Math.min(bytes.length, Short.MAX_VALUE);

        int required = 2 + len;

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