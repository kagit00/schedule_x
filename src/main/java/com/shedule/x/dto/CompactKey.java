package com.shedule.x.dto;

import java.nio.ByteBuffer;
import java.util.UUID;

public record CompactKey(
    UUID groupId,
    int chunkIndex,
    UUID referenceId,
    UUID matchedReferenceId
) {
    public byte[] toBytes() {
        return ByteBuffer.allocate(48)
            .putLong(groupId.getMostSignificantBits())
            .putLong(groupId.getLeastSignificantBits())
            .putInt(chunkIndex)
            .putLong(referenceId.getMostSignificantBits())
            .putLong(referenceId.getLeastSignificantBits())
            .putLong(matchedReferenceId.getMostSignificantBits())
            .putLong(matchedReferenceId.getLeastSignificantBits())
            .array();
    }

    public static CompactKey fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return new CompactKey(
            new UUID(buf.getLong(), buf.getLong()),
            buf.getInt(),
            new UUID(buf.getLong(), buf.getLong()),
            new UUID(buf.getLong(), buf.getLong())
        );
    }
}