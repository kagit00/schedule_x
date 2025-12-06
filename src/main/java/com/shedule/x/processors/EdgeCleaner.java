package com.shedule.x.processors;

import java.nio.ByteBuffer;
import java.util.function.Predicate;

public interface EdgeCleaner {
    void deleteByPrefix(ByteBuffer prefix);
    void deleteByPrefix(ByteBuffer prefix, Predicate<ByteBuffer> matcher);
}