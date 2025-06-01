package com.shedule.x.utils;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.db.BatchUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@UtilityClass
public final class GraphStoreUtils {
    private static final int DEFAULT_BATCH_SIZE = 250;
    private static final int RETRY_ATTEMPTS = 3;
    private static final long TIMEOUT_SECONDS = 30;
    private static final int BASE_BACKOFF_MS = 100;
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;

    private static final String CLOSED_DB_MESSAGE = "DB is closed for groupId={}";
    private static final String CORRUPTION_MESSAGE = "Detected MapDB corruption for groupId={}, chunkIndex={}";

    public static void validateDbPath(File parentDir) throws IOException {
        if (parentDir == null) {
            throw new IOException("Invalid dbPath, no parent directory");
        }
        if (!parentDir.exists() && !parentDir.mkdirs()) {
            throw new IOException("Failed to create directory: " + parentDir.getAbsolutePath());
        }
        if (!parentDir.canWrite()) {
            throw new IOException("No write permission for directory: " + parentDir.getAbsolutePath());
        }
    }

    public static void registerMetrics(MeterRegistry meterRegistry, Map<String, HTreeMap<String, byte[]>> groupMaps,
                                       ExecutorService mapdbExecutor, ExecutorService commitExecutor,
                                       AtomicInteger pendingCommits) {
        meterRegistry.gauge("mapdb_total_map_size", groupMaps, maps -> maps.values().stream()
                .filter(map -> !map.isClosed())
                .mapToLong(HTreeMap::size)
                .sum());
        meterRegistry.gauge("mapdb_executor_queue", mapdbExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("mapdb_executor_active", mapdbExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
        meterRegistry.gauge("mapdb_commit_executor_queue", commitExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
        meterRegistry.gauge("mapdb_pending_commits", pendingCommits, AtomicInteger::get);
    }

    public static void logAndCountError(MeterRegistry meterRegistry, String metricName, String groupId, int count, String message, Object... args) {
        log.error(message, args);
        meterRegistry.counter(metricName, "groupId", groupId).increment(count);
    }

    public static CompletableFuture<Void> persistEdgesAsync(List<GraphRecords.PotentialMatch> matches,
                                                            String groupId, int chunkIndex,
                                                            Map<String, HTreeMap<String, byte[]>> groupMaps,
                                                            ExecutorService mapdbExecutor,
                                                            ExecutorService commitExecutor,
                                                            MeterRegistry meterRegistry,
                                                            int batchSize,
                                                            AtomicInteger pendingCommits,
                                                            DB db) {
        if (matches.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        HTreeMap<String, byte[]> map = getOrCreateMap(groupMaps, groupId, db, meterRegistry);
        batchSize = Math.max(batchSize, DEFAULT_BATCH_SIZE);
        Instant submitStart = Instant.now();

        List<CompletableFuture<Void>> batchFutures = BatchUtils.partition(matches, batchSize)
                .stream()
                .map(chunk -> processChunkAsync(chunk, groupId, chunkIndex, map, meterRegistry, mapdbExecutor, db))
                .toList();

        return CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]))
                .thenRunAsync(() -> commitChanges(db, groupId, chunkIndex, meterRegistry, pendingCommits), commitExecutor)
                .whenComplete((v, e) -> finalizePersist(submitStart, groupId, chunkIndex, matches.size(), meterRegistry, e));
    }

    @SuppressWarnings("unchecked")
    private static HTreeMap<String, byte[]> getOrCreateMap(Map<String, HTreeMap<String, byte[]>> groupMaps,
                                                           String groupId, DB db, MeterRegistry meterRegistry) {
        if (db.isClosed()) {
            logAndCountError(meterRegistry, "mapdb_store_closed_errors", groupId, 1,
                    "Cannot persist edges: DB is closed for groupId={}", groupId);
            throw new IllegalStateException("Cannot persist edges: DB is closed");
        }

        HTreeMap<String, byte[]> map = groupMaps.computeIfAbsent(groupId, id -> {
            if (db.isClosed()) {
                throw new IllegalStateException("Cannot create map: DB is closed");
            }
            return (HTreeMap<String, byte[]>) db.hashMap("edges_" + id).createOrOpen();
        });

        if (map.isClosed()) {
            logAndCountError(meterRegistry, "mapdb_store_closed_errors", groupId, 1,
                    "Cannot persist edges: Map is closed for groupId={}", groupId);
            throw new IllegalStateException("Cannot persist edges: Map is closed");
        }

        return map;
    }

    private static CompletableFuture<Void> processChunkAsync(List<GraphRecords.PotentialMatch> chunk,
                                                             String groupId, int chunkIndex,
                                                             HTreeMap<String, byte[]> map,
                                                             MeterRegistry meterRegistry,
                                                             ExecutorService mapdbExecutor,
                                                             DB db) {
        return CompletableFuture.runAsync(() -> {
            try {
                RetryUtils.persistChunkWithRetry(chunk, groupId, chunkIndex, map, meterRegistry,
                        RETRY_ATTEMPTS, BASE_BACKOFF_MS, db);
            } catch (Exception e) {
                logAndCountError(meterRegistry, "mapdb_persist_errors", groupId, chunk.size(),
                        "Batch persist failed for groupId={}, chunkIndex={}, size={}", groupId, chunkIndex, chunk.size(), e);
                throw new CompletionException("Failed to persist chunk for groupId=" + groupId, e);
            }
        }, mapdbExecutor).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void commitChanges(DB db, String groupId, int chunkIndex, MeterRegistry meterRegistry,
                                      AtomicInteger pendingCommits) {
        pendingCommits.incrementAndGet();
        try {
            if (!db.isClosed()) {
                Instant commitStart = Instant.now();
                db.commit();
                meterRegistry.counter("mapdb_commits", "groupId", groupId).increment();
                meterRegistry.timer("mapdb_commit_latency", "groupId", groupId)
                        .record(Duration.between(commitStart, Instant.now()));
            }
        } catch (DBException e) {
            logAndCountError(meterRegistry, "mapdb_commit_errors", groupId, 1,
                    "Commit failed for groupId={}, chunkIndex={}", groupId, chunkIndex, e);
            throw e;
        } finally {
            pendingCommits.decrementAndGet();
        }
    }

    private static void finalizePersist(Instant submitStart, String groupId, int chunkIndex, int matchCount,
                                        MeterRegistry meterRegistry, Throwable error) {
        Duration duration = Duration.between(submitStart, Instant.now());
        meterRegistry.timer("mapdb_persist_submit_latency", "groupId", groupId).record(duration);
        if (error != null) {
            logAndCountError(meterRegistry, "mapdb_persist_errors", groupId, matchCount,
                    "Failed to persist edges for groupId={}, chunkIndex={}, matches={}", groupId, chunkIndex, matchCount, error);
        } else {
            log.debug("Persisted {} edges for groupId={}, chunkIndex={} in {} ms", matchCount, groupId, chunkIndex, duration.toMillis());
        }
    }

    public static AutoCloseableStream<Edge> streamEdges(UUID domainId, String groupId,
                                                        Map<String, HTreeMap<String, byte[]>> groupMaps,
                                                        MeterRegistry meterRegistry, int batchSize) {
        Instant streamStart = Instant.now();
        HTreeMap<String, byte[]> map = getMap(groupMaps, groupId, meterRegistry);
        if (map == null) {
            return new AutoCloseableStream<>(Stream.empty());
        }

        return new AutoCloseableStream<>(StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(createEdgeIterator(map, groupId, domainId, meterRegistry, batchSize), Spliterator.ORDERED),
                        false)
                .onClose(() -> meterRegistry.timer("mapdb_stream_latency", "groupId", groupId)
                        .record(Duration.between(streamStart, Instant.now()))));
    }

    private static HTreeMap<String, byte[]> getMap(Map<String, HTreeMap<String, byte[]>> groupMaps,
                                                   String groupId, MeterRegistry meterRegistry) {
        HTreeMap<String, byte[]> map = groupMaps.get(groupId);
        if (map == null || map.isClosed()) {
            meterRegistry.counter("mapdb_stream_empty", "groupId", groupId).increment();
            return null;
        }
        return map;
    }

    private static Iterator<Edge> createEdgeIterator(HTreeMap<String, byte[]> map, String groupId, UUID domainId,
                                                     MeterRegistry meterRegistry, int batchSize) {
        return new Iterator<>() {
            private final Iterator<String> keyIterator = map.keySet().iterator();
            private final List<Edge> buffer = new ArrayList<>(batchSize);
            private int bufferIndex = 0;

            @Override
            public boolean hasNext() {
                if (bufferIndex < buffer.size()) {
                    return true;
                }
                buffer.clear();
                bufferIndex = 0;
                int count = 0;
                while (keyIterator.hasNext() && count < batchSize) {
                    String key = keyIterator.next();
                    byte[] data = map.get(key);
                    if (data != null) {
                        try {
                            GraphRecords.PotentialMatch match = MatchSerializer.deserialize(data, groupId, domainId);
                            buffer.add(GraphRequestFactory.toEdge(match));
                            meterRegistry.counter("mapdb_edges_streamed", "groupId", groupId).increment();
                        } catch (Exception e) {
                            logAndCountError(meterRegistry, "mapdb_deserialize_errors", groupId, 1,
                                    "Failed to deserialize match for key={} in groupId={}", key, groupId, e);
                        }
                    }
                    count++;
                }
                return !buffer.isEmpty();
            }

            @Override
            public Edge next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return buffer.get(bufferIndex++);
            }
        };
    }

    public static void cleanEdges(String groupId, Map<String, HTreeMap<String, byte[]>> groupMaps, DB db,
                                  ExecutorService cleanupExecutor, MeterRegistry meterRegistry, int batchSize) {
        CompletableFuture.runAsync(() -> {
            Instant cleanStart = Instant.now();
            HTreeMap<String, byte[]> map;
            synchronized (groupMaps) {
                map = groupMaps.get(groupId);
            }
            if (map == null || map.isClosed()) {
                meterRegistry.counter("mapdb_clean_skipped", "groupId", groupId).increment();
                return;
            }

            Iterator<String> keyIterator = map.keySet().iterator();
            List<String> batch = new ArrayList<>(batchSize);
            long cleanedCount = 0;

            while (keyIterator.hasNext()) {
                String key = keyIterator.next();
                batch.add(key);
                if (batch.size() >= batchSize) {
                    batch.forEach(map::remove);
                    if (!db.isClosed()) {
                        db.commit();
                    }
                    cleanedCount += batch.size();
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                batch.forEach(map::remove);
                if (!db.isClosed()) {
                    db.commit();
                }
                cleanedCount += batch.size();
            }
            synchronized (groupMaps) {
                HTreeMap<String, byte[]> removed = groupMaps.remove(groupId);
                if (!map.isClosed()) {
                    map.close();
                }
            }
            meterRegistry.counter("mapdb_edges_cleaned", "groupId", groupId).increment(cleanedCount);
            meterRegistry.timer("mapdb_clean_latency", "groupId", groupId)
                    .record(Duration.between(cleanStart, Instant.now()));
        }, cleanupExecutor).orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public static List<String> listGroupIds(Map<String, HTreeMap<String, byte[]>> groupMaps) {
        try {
            synchronized (groupMaps) {
                return new ArrayList<>(groupMaps.keySet());
            }
        } catch (Exception e) {
            log.error("Failed to list groupIds", e);
            throw new InternalServerErrorException("Failed to list groupIds");
        }
    }

    public static void close(Map<String, HTreeMap<String, byte[]>> groupMaps, DB db,
                             ExecutorService mapdbExecutor, ExecutorService commitExecutor,
                             ExecutorService cleanupExecutor) {
        try {
            // Wait for pending tasks to complete
            cleanupExecutor.shutdown();
            commitExecutor.shutdown();
            mapdbExecutor.shutdown();
            boolean cleanupTerminated = cleanupExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            boolean commitTerminated = commitExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            boolean mapdbTerminated = mapdbExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (!cleanupTerminated) {
                cleanupExecutor.shutdownNow();
                log.warn("Cleanup executor did not terminate gracefully");
            }
            if (!commitTerminated) {
                commitExecutor.shutdownNow();
                log.warn("Commit executor did not terminate gracefully");
            }
            if (!mapdbTerminated) {
                mapdbExecutor.shutdownNow();
                log.warn("MapDB executor did not terminate gracefully");
            }

            synchronized (groupMaps) {
                groupMaps.forEach((groupId, map) -> {
                    try {
                        if (!map.isClosed()) {
                            map.close();
                        }
                    } catch (Exception e) {
                        log.error("Failed to close map for groupId={}", groupId, e);
                    }
                });
                groupMaps.clear();
            }

            if (db != null && !db.isClosed()) {
                db.close();
            }
            log.info("Closed MapDB resources");
        } catch (Exception e) {
            log.error("Failed to close MapDB resources", e);
        }
    }

    public static void checkDbIntegrity(DB db, Map<String, HTreeMap<String, byte[]>> groupMaps, String groupId, int chunkIndex, MeterRegistry meterRegistry) {
        validateDbState(db, groupId, meterRegistry);
        HTreeMap<String, byte[]> map = groupMaps.get(groupId);
        if (map == null || map.isClosed()) {
            return;
        }

        try {
            verifyMapIntegrity(map, groupId);
            log.debug("GroupId={} integrity check passed", groupId);
        } catch (DBException.DataCorruption | NullPointerException e) {
            logAndCountError(meterRegistry, "mapdb_corruption_errors", groupId, 1, CORRUPTION_MESSAGE, groupId, chunkIndex, e);
            throw new InternalServerErrorException("MapDB index corruption detected");
        }
    }

    private static void validateDbState(DB db, String groupId, MeterRegistry meterRegistry) {
        if (db == null || db.isClosed()) {
            logAndCountError(meterRegistry, "mapdb_store_closed_errors", groupId, 1, CLOSED_DB_MESSAGE, groupId);
            throw new IllegalStateException("DB is closed");
        }
    }

    private static void verifyMapIntegrity(HTreeMap<String, byte[]> map, String groupId) {
        Iterator<String> iterator = map.keySet().iterator();
        if (iterator.hasNext()) {
            iterator.next();
        }
    }
}