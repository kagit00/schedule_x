package com.shedule.x.processors;

import java.nio.ByteBuffer;
import java.util.UUID;

public interface KeyPrefixProvider {
    ByteBuffer makePrefix(UUID groupId, String cycleId);
    ByteBuffer makePrefix(UUID groupId);
    ByteBuffer makePrefix(Object... parts);
    boolean matchesPrefix(ByteBuffer key, Object... expectedParts);
}