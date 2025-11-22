package com.shedule.x.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

@Slf4j
@Component
@RequiredArgsConstructor
public class LSHMaintenance {
    private static final long BUCKET_TTL_NS = 86_400_000_000_000L;
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> lshDbi;

    @Scheduled(cron = "0 4 * * * *")  // 4 AM daily
    public void nightlyLshMaintenance() {
        log.info("Starting nightly LSH maintenance...");
        long start = System.currentTimeMillis();

        // Optional: clean very old buckets using your TTL logic
        try (Txn<ByteBuffer> txn = env.txnRead();
             Cursor<ByteBuffer> cursor = lshDbi.openCursor(txn)) {

            int cleaned = 0;
            while (cursor.next()) {
                ByteBuffer val = cursor.val();
                if (val != null && val.remaining() >= 12) {
                    long ts = val.duplicate().order(ByteOrder.BIG_ENDIAN).getLong(0);
                    if (System.nanoTime() - ts > BUCKET_TTL_NS) {
                        cursor.delete();
                        cleaned++;
                    }
                }
            }
            if (cleaned > 0) {
                log.info("Nightly LSH cleanup: removed {} expired buckets", cleaned);
            }
        } catch (Exception e) {
            log.warn("Nightly LSH cleanup failed", e);
        }

        log.info("Nightly LSH maintenance completed in {}ms", System.currentTimeMillis() - start);
    }
}
