package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.*;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Component
@Slf4j
@RequiredArgsConstructor
public class LmdbEdgeReader implements EdgeReader {

    private final LmdbEnvironment lmdb;
    private final MeterRegistry meters;

    // Thread-local buffers for reads (same as original)
    private static final ThreadLocal<ByteBuffer> KEY_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(512).order(ByteOrder.BIG_ENDIAN));
    private static final ThreadLocal<ByteBuffer> VAL_BUF = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(1024).order(ByteOrder.BIG_ENDIAN));

    @Override
    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, ByteBuffer prefix,
                                                    Predicate<ByteBuffer> prefixMatcher) {
        Env<ByteBuffer> env = lmdb.env();
        Txn<ByteBuffer> txn = env.txnRead();
        Dbi<ByteBuffer> dbi = lmdb.edgeDbi();

        try {
            var cursor = dbi.iterate(txn, KeyRange.atLeast(prefix));
            Iterator<CursorIterable.KeyVal<ByteBuffer>> kvIter = cursor.iterator();

            Iterator<EdgeDTO> edgeIter = new Iterator<>() {
                EdgeDTO nextItem = null;
                boolean done = false;

                @Override public boolean hasNext() {
                    if (nextItem != null) return true;
                    if (done) return false;
                    nextItem = advance();
                    if (nextItem == null) done = true;
                    return nextItem != null;
                }

                @Override public EdgeDTO next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    EdgeDTO cur = nextItem;
                    nextItem = null;
                    return cur;
                }

                private EdgeDTO advance() {
                    while (kvIter.hasNext()) {
                        var kv = kvIter.next();
                        ByteBuffer key = kv.key();
                        if (!prefixMatcher.test(key)) return null;

                        ByteBuffer val = kv.val();
                        if (!domainMatches(val, domainId)) continue;

                        return decodeEdge(key, val, domainId);
                    }
                    return null;
                }
            };

            Stream<EdgeDTO> stream = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(edgeIter, Spliterator.ORDERED | Spliterator.NONNULL),
                    false);

            return new AutoCloseableStream<>(stream, () -> {
                try { cursor.close(); } finally { txn.close(); }
            });
        } catch (Exception e) {
            txn.close();
            throw new RuntimeException("Failed to stream edges", e);
        }
    }

    private boolean domainMatches(ByteBuffer val, UUID domainId) {
        val.position(4); // skip float score
        long msb = val.getLong();
        long lsb = val.getLong();
        return msb == domainId.getMostSignificantBits() && lsb == domainId.getLeastSignificantBits();
    }

    private EdgeDTO decodeEdge(ByteBuffer key, ByteBuffer val, UUID domainId) {
        val.position(0);
        float score = val.getFloat();
        UUID domain = getUUID(val);
        String from = readString(val);
        String to = readString(val);

        return EdgeDTO.builder()
                .fromNodeHash(from)
                .toNodeHash(to)
                .score(score)
                .domainId(domain)
                .groupId(extractGroupId(key))
                .build();
    }

    private UUID extractGroupId(ByteBuffer key) {
        return new UUID(key.getLong(0), key.getLong(8));
    }

    private static String readString(ByteBuffer bb) {
        int len = bb.getInt();
        byte[] arr = new byte[len];
        bb.get(arr);
        return new String(arr, StandardCharsets.UTF_8);
    }

    private static UUID getUUID(ByteBuffer bb) {
        return new UUID(bb.getLong(), bb.getLong());
    }
}