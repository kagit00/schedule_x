package com.shedule.x.processors;

import com.shedule.x.utils.graph.StoreUtility;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.*;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Predicate;

@Component
@Slf4j
@RequiredArgsConstructor
public class LmdbEdgeCleaner implements EdgeCleaner {

    private final LmdbEnvironment lmdb;
    private final MeterRegistry meters;

    private static final ThreadLocal<ByteBuffer> SCRATCH_KEY = ThreadLocal.withInitial(
            () -> ByteBuffer.allocateDirect(512).order(ByteOrder.BIG_ENDIAN)
    );

    @Override
    public void deleteByPrefix(ByteBuffer prefix) {
        deleteByPrefix(prefix, k -> true);
    }

    @Override
    public void deleteByPrefix(ByteBuffer prefix, Predicate<ByteBuffer> matcher) {
        Txn<ByteBuffer> txn = null;
        Cursor<ByteBuffer> cur = null;
        int deleted = 0;

        try {
            Env<ByteBuffer> env = lmdb.env();
            Dbi<ByteBuffer> dbi = lmdb.edgeDbi();

            txn = env.txnWrite();
            cur = dbi.openCursor(txn);

            boolean found = cur.get(prefix, GetOp.MDB_SET_RANGE);

            while (found) {
                ByteBuffer key = cur.key();
                if (key == null) break;

                if (!StoreUtility.keyStartsWith(key, prefix)) {
                    break;
                }

                ByteBuffer copy = SCRATCH_KEY.get();
                copy.clear();
                copy.put(key.duplicate());
                copy.flip();

                if (matcher.test(copy)) {
                    cur.delete();
                    deleted++;
                }

                found = cur.next();
            }

            meters.counter("edges_cleaned").increment(deleted);

            txn.commit();

        } catch (Exception e) {
            log.warn("Edge cleanup failed (deleted so far: {})", deleted, e);
        } finally {
            if (cur != null) {
                try {
                    cur.close();
                } catch (Txn.NotReadyException nre) {
                    log.debug("Cursor close hit NotReady txn (already completed)", nre);
                } catch (Exception e) {
                    log.warn("Error closing cursor in edge cleaner", e);
                }
            }
            if (txn != null) {
                try {
                    txn.close();
                } catch (Exception e) {
                    log.warn("Error closing txn in edge cleaner", e);
                }
            }
        }
    }
}