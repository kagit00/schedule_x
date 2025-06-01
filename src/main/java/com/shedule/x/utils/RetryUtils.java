package com.shedule.x.utils;

import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.HTreeMap;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

@Slf4j
@UtilityClass
public final class RetryUtils {
    private static final String CLOSED_RESOURCE_MESSAGE = "Cannot persist chunk: DB or map is closed for groupId={}, chunkIndex={}";
    private static final String PERSIST_RETRY_MESSAGE = "Retrying chunk persist for groupId={}, chunkIndex={}, attempt {}/{} due to {}";
    private static final String PERSIST_FAILED_MESSAGE = "Failed to persist chunk for groupId={}, chunkIndex={} after {} attempts due to {}";
    private static final String UNEXPECTED_ERROR_MESSAGE = "Unexpected error persisting chunk for groupId={}, chunkIndex={}";

    public static void persistChunkWithRetry(List<GraphRecords.PotentialMatch> chunk, String groupId, int chunkIndex,
                                             HTreeMap<String, byte[]> map, MeterRegistry meterRegistry,
                                             int maxAttempts, long baseBackoffMs, DB db) {
        int attempt = 0;
        while (attempt < maxAttempts) {
            try {
                validateResources(db, map, groupId, chunkIndex, meterRegistry);
                writeChunk(chunk, groupId, chunkIndex, map, meterRegistry);
                return;
            } catch (IllegalAccessError | IllegalStateException e) {
                GraphStoreUtils.logAndCountError(meterRegistry, "mapdb_store_closed_errors", groupId, chunk.size(),
                        CLOSED_RESOURCE_MESSAGE, groupId, chunkIndex, e);
                throw e; // Fail fast on closed resources
            } catch (NullPointerException e) {
                if (++attempt >= maxAttempts) {
                    GraphStoreUtils.logAndCountError(meterRegistry, "mapdb_persist_npe_errors", groupId, chunk.size(),
                            PERSIST_FAILED_MESSAGE, groupId, chunkIndex, maxAttempts, "MapDB NPE", e);
                    throw e;
                }
                GraphStoreUtils.logAndCountError(meterRegistry, "mapdb_persist_retries", groupId, 1,
                        PERSIST_RETRY_MESSAGE, groupId, chunkIndex, attempt, maxAttempts, "MapDB NPE", e);
                applyBackoff(attempt, baseBackoffMs);
            } catch (DBException.DataCorruption | DBException.GetVoid e) {
                if (++attempt >= maxAttempts) {
                    GraphStoreUtils.logAndCountError(meterRegistry, "mapdb_persist_errors", groupId, chunk.size(),
                            PERSIST_FAILED_MESSAGE, groupId, chunkIndex, maxAttempts, e.getClass().getSimpleName(), e);
                    throw e;
                }
                GraphStoreUtils.logAndCountError(meterRegistry, "mapdb_persist_retries", groupId, 1,
                        PERSIST_RETRY_MESSAGE, groupId, chunkIndex, attempt, maxAttempts, e.getClass().getSimpleName(), e);
                applyBackoff(attempt, baseBackoffMs);
            } catch (Exception e) {
                GraphStoreUtils.logAndCountError(meterRegistry, "mapdb_persist_errors", groupId, chunk.size(),
                        UNEXPECTED_ERROR_MESSAGE, groupId, chunkIndex, e);
                throw e;
            }
        }
    }

    private static void validateResources(DB db, HTreeMap<String, byte[]> map, String groupId, int chunkIndex,
                                          MeterRegistry meterRegistry) {
        if (db == null || db.isClosed() || map == null || map.isClosed()) {
            GraphStoreUtils.logAndCountError(meterRegistry, "mapdb_store_closed_errors", groupId, 1,
                    CLOSED_RESOURCE_MESSAGE, groupId, chunkIndex);
            throw new IllegalStateException("DB or map is closed");
        }
        GraphStoreUtils.checkDbIntegrity(db, Collections.singletonMap(groupId, map), groupId, chunkIndex, meterRegistry);
    }

    private static void writeChunk(List<GraphRecords.PotentialMatch> chunk, String groupId, int chunkIndex,
                                   HTreeMap<String, byte[]> map, MeterRegistry meterRegistry) {
        Instant writeStart = Instant.now();
        for (GraphRecords.PotentialMatch match : chunk) {
            String key = String.format("%s:%d:%s:%s", groupId, chunkIndex, match.getReferenceId(), match.getMatchedReferenceId());
            map.put(key, MatchSerializer.serialize(match));
        }
        meterRegistry.counter("mapdb_edges_persisted", "groupId", groupId).increment(chunk.size());
        meterRegistry.timer("mapdb_persist_latency", "groupId", groupId)
                .record(Duration.between(writeStart, Instant.now()));
    }

    private static void applyBackoff(int attempt, long baseBackoffMs) {
        try {
            Thread.sleep(baseBackoffMs * (1L << attempt)); // Exponential backoff
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry backoff", ie);
        }
    }
}