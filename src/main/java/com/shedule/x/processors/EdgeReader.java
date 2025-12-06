package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Predicate;

public interface EdgeReader {
    AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, ByteBuffer prefix,
                                             Predicate<ByteBuffer> prefixMatcher);
}