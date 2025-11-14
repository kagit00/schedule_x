package com.shedule.x.processors;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.shedule.x.config.factory.SerializerContext;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.mapdb.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.IOException;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.mapdb.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


import org.mapdb.*;
import java.util.*;
import java.util.concurrent.*;
import org.mapdb.*;
import java.nio.charset.StandardCharsets;

import java.util.*;
import java.util.concurrent.*;
import static com.shedule.x.utils.basic.ByteUtils.*;


@Slf4j
@Component
public class GraphStore implements AutoCloseable {

    final int LSH_MAGIC = 0x4C534800;
    private static final int MAX_BUCKET_DATA_SIZE = 10 * 1024 * 1024; // 10MB
    private static final int MAX_REMAINING_DATA_SIZE = 5 * 1024 * 1024; // 5MB
    private static final int MAX_STRING_LENGTH = 65535;

    private DB db;
    private HTreeMap<byte[], byte[]> map;
    private final ExecutorService mapdbExecutor;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger pendingCommits = new AtomicInteger(0);
    private final String dbPath;
    private final int batchSize;

    private static final int BUCKET_SIZE_LIMIT = 50_000;
    private static final int BUCKET_TRIM_TO = 45_000;
    private static final long MAX_TOTAL_BUCKETS = 5_000_000L;
    private static final long BUCKET_TTL_MS = 24 * 60 * 60 * 1000L;

    private final AtomicLong totalBucketEntries = new AtomicLong(0);

    public GraphStore(
            @Qualifier("persistenceExecutor") ExecutorService mapdbExecutor,
            MeterRegistry meterRegistry,
            @Value("${graph.store.path:/app/graph-store}") String dbPath,
            @Value("${mapdb.batch-size:500}") int batchSize) {
        this.mapdbExecutor = mapdbExecutor;
        this.meterRegistry = meterRegistry;
        this.dbPath = dbPath;
        this.batchSize = batchSize;
    }

    @PostConstruct
    public void init() {
        try {
            if (dbPath == null || dbPath.trim().isEmpty()) {
                throw new IllegalArgumentException("graph.store.path is null or empty");
            }
            File dbFile = new File(dbPath, "graphstore.db");
            File parentDir = dbFile.getParentFile();

            if (parentDir == null) {
                throw new IOException("Invalid dbPath, no parent directory: " + dbPath);
            }
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                throw new IOException("Failed to create directory: " + parentDir.getAbsolutePath());
            }
            if (!parentDir.canWrite()) {
                throw new IOException("No write permission for directory: " + parentDir.getAbsolutePath());
            }

            this.db = DBMaker.fileDB(dbFile)
                    .fileMmapEnable()
                    .fileMmapPreclearDisable()
                    .cleanerHackEnable()
                    .allocateStartSize(512 * 1024 * 1024L)
                    .allocateIncrement(256 * 1024 * 1024L)
                    .transactionEnable()
                    .concurrencyScale(32)
                    .make();

            this.map = db.hashMap("graph-store")
                    .keySerializer(Serializer.BYTE_ARRAY)
                    .valueSerializer(Serializer.BYTE_ARRAY)
                    .createOrOpen();

            meterRegistry.gauge("mapdb_map_size", map, m -> (long) m.size());
            meterRegistry.gauge("mapdb_executor_queue", mapdbExecutor,
                    exec -> ((ThreadPoolExecutor) exec).getQueue().size());
            meterRegistry.gauge("mapdb_executor_active", mapdbExecutor,
                    exec -> ((ThreadPoolExecutor) exec).getActiveCount());
            meterRegistry.gauge("mapdb_pending_commits", pendingCommits, AtomicInteger::get);
            meterRegistry.gauge("lsh_total_bucket_entries", totalBucketEntries, AtomicLong::get);

            new JvmMemoryMetrics().bindTo(meterRegistry);
            new JvmGcMetrics().bindTo(meterRegistry);

            log.info("Initialized MapDB with variable-length binary keys at path={}", dbFile.getAbsolutePath());
        } catch (Exception e) {
            log.error("Failed to initialize MapDB at path={}: {}", dbPath, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            if (map != null && !map.isClosed()) {
                map.close();
            }
            if (db != null && !db.isClosed()) {
                db.commit();
                db.close();
            }
            log.info("Closed MapDB");
        } catch (Exception e) {
            log.error("Failed to close MapDB: {}", e.getMessage(), e);
        }
    }

    public CompletableFuture<Void> persistEdgesAsync(List<GraphRecords.PotentialMatch> matches,
                                                     UUID groupId, int chunkIndex) {
        if (matches.isEmpty()) {
            log.debug("No matches to persist for groupId={}, chunkIndex={}", groupId, chunkIndex);
            return CompletableFuture.completedFuture(null);
        }

        Instant submitStart = Instant.now();
        int dynamicBatchSize = adjustBatchSize(matches.size());
        List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

        for (List<GraphRecords.PotentialMatch> subBatch : ListUtils.partition(matches, dynamicBatchSize)) {
            batchFutures.add(CompletableFuture.runAsync(
                    () -> persistBatchWithCommit(subBatch, groupId, chunkIndex),
                    mapdbExecutor).whenComplete((v, e) -> {
                if (e != null) {
                    log.error("Failed to persist batch for groupId={}, chunkIndex={}: {}",
                            groupId, chunkIndex, e, e);
                    meterRegistry.counter("mapdb_persist_batch_errors",
                            "groupId", groupId.toString()).increment(subBatch.size());
                }
            }));
        }

        return CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]))
                .whenComplete((v, e) -> {
                    meterRegistry.timer("mapdb_persist_submit_latency",
                                    "groupId", groupId.toString())
                            .record(Duration.between(submitStart, Instant.now()));
                    if (e == null) {
                        meterRegistry.counter("mapdb_edges_persisted",
                                "groupId", groupId.toString()).increment(matches.size());
                    } else {
                        meterRegistry.counter("mapdb_persist_errors",
                                "groupId", groupId.toString()).increment(matches.size());
                    }
                });
    }

    private void persistBatchWithCommit(List<GraphRecords.PotentialMatch> subBatch,
                                        UUID groupId, int chunkIndex) {
        try {
            Instant writeStart = Instant.now();
            for (GraphRecords.PotentialMatch match : subBatch) {
                byte[] key = compactKey(groupId, chunkIndex, match);
                map.put(key, serializeMatch(match));
            }
            db.commit();
            pendingCommits.set(0);

            long durationMs = Duration.between(writeStart, Instant.now()).toMillis();
            meterRegistry.timer("mapdb_persist_latency", "groupId", groupId.toString())
                    .record(Duration.ofMillis(durationMs));
            meterRegistry.counter("mapdb_edges_persisted", "groupId", groupId.toString())
                    .increment(subBatch.size());
            meterRegistry.counter("mapdb_commits", "groupId", groupId.toString()).increment();

            log.debug("Persisted & committed {} edges for groupId={}, chunkIndex={} in {} ms",
                    subBatch.size(), groupId, chunkIndex, durationMs);
            if (durationMs > 10_000) {
                log.warn("Slow persist+commit: {} edges, {} ms, groupId={}",
                        subBatch.size(), durationMs, groupId);
            }
        } catch (Exception e) {
            log.error("Failed to persist batch for groupId={}, chunkIndex={}: {}",
                    groupId, chunkIndex, e, e);
            meterRegistry.counter("mapdb_persist_errors", "groupId", groupId.toString())
                    .increment(subBatch.size());
            throw new RuntimeException(e);
        }
    }

    private int adjustBatchSize(int totalEdges) {
        int queueSize = ((ThreadPoolExecutor) mapdbExecutor).getQueue().size();
        int base = batchSize;
        if (queueSize > 200) {
            base = Math.max(50, base / 4);
        } else if (queueSize > 100) {
            base = Math.max(100, base / 2);
        }
        return Math.min(base, totalEdges);
    }

    public CompletableFuture<Void> persistEdgesAsyncFallback(List<GraphRecords.PotentialMatch> matches,
                                                             String groupId, int chunkIndex, Throwable t) {
        log.warn("Persist failed for groupId={}, chunkIndex={}: {}", groupId, chunkIndex, t.getMessage());
        meterRegistry.counter("mapdb_persist_fallbacks", "groupId", groupId).increment(matches.size());
        return CompletableFuture.completedFuture(null);
    }

    public AutoCloseableStream<Edge> streamEdgesFallback(UUID domainId, UUID groupId,
                                                         int topK, Throwable t) {
        log.warn("Stream failed for groupId={}: {}", groupId, t.getMessage());
        meterRegistry.counter("mapdb_stream_fallbacks", "groupId", groupId.toString()).increment();
        return new AutoCloseableStream<>(java.util.stream.Stream.empty());
    }

    public void cleanEdgesFallback(UUID groupId, Throwable t) {
        log.warn("Clean failed for groupId={}: {}", groupId, t.getMessage());
        meterRegistry.counter("mapdb_clean_fallbacks", "groupId", groupId.toString()).increment();
    }

    public List<UUID> listGroupIds() {
        try {
            Set<UUID> groups = new HashSet<>();
            for (Map.Entry<byte[], byte[]> entry : map.getEntries()) {
                byte[] key = entry.getKey();
                if (key.length >= 16) {
                    long msb = getLong(key, 0);
                    long lsb = getLong(key, 8);
                    groups.add(new UUID(msb, lsb));
                }
            }
            return new ArrayList<>(groups);
        } catch (Exception e) {
            log.error("Failed to list groupIds: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public AutoCloseableStream<Edge> streamEdges(UUID domainId, UUID groupId) {
        if (domainId == null) throw new IllegalArgumentException("domainId must not be null");

        Instant start = Instant.now();
        long groupMsb = groupId.getMostSignificantBits();
        long groupLsb = groupId.getLeastSignificantBits();

        java.util.stream.Stream<Edge> stream = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                new Iterator<Edge>() {
                                    private final Iterator<HTreeMap.Entry<byte[], byte[]>> iter = map.getEntries().iterator();
                                    private Edge nextEdge = null;

                                    @Override
                                    public boolean hasNext() {
                                        if (nextEdge != null) return true;
                                        while (iter.hasNext()) {
                                            HTreeMap.Entry<byte[], byte[]> entry = iter.next();
                                            byte[] key = entry.getKey();
                                            if (key.length < 20) continue;

                                            long msb = getLong(key, 0);
                                            long lsb = getLong(key, 8);
                                            if (msb != groupMsb || lsb != groupLsb) continue;

                                            try {
                                                GraphRecords.PotentialMatch match = deserializeMatch(entry.getValue(), groupId, domainId);
                                                if (!domainId.equals(match.getDomainId())) continue;

                                                nextEdge = toEdge(match);
                                                meterRegistry.counter("mapdb_edges_streamed", "groupId", groupId.toString()).increment();
                                                return true;
                                            } catch (Exception e) {
                                                log.error("Failed to deserialize during stream", e);
                                                meterRegistry.counter("mapdb_deserialize_errors", "groupId", groupId.toString()).increment();
                                            }
                                        }
                                        return false;
                                    }

                                    @Override
                                    public Edge next() {
                                        if (!hasNext()) throw new NoSuchElementException();
                                        Edge result = nextEdge;
                                        nextEdge = null;
                                        return result;
                                    }
                                }, Spliterator.NONNULL), false)
                .onClose(() -> {
                    long durationMs = Duration.between(start, Instant.now()).toMillis();
                    meterRegistry.timer("mapdb_stream_latency", "groupId", groupId.toString())
                            .record(Duration.ofMillis(durationMs));
                    log.info("Streamed edges for groupId={} in {} ms", groupId, durationMs);
                });

        return new AutoCloseableStream<>(stream);
    }

    public void cleanEdges(UUID groupId) {
        Instant start = Instant.now();
        long msb = groupId.getMostSignificantBits();
        long lsb = groupId.getLeastSignificantBits();

        Iterator<HTreeMap.Entry<byte[], byte[]>> iter = map.getEntries().iterator();
        List<byte[]> batch = new ArrayList<>(batchSize / 2);
        int removed = 0;

        while (iter.hasNext()) {
            HTreeMap.Entry<byte[], byte[]> entry = iter.next();
            byte[] key = entry.getKey();
            if (key.length >= 16 &&
                    getLong(key, 0) == msb &&
                    getLong(key, 8) == lsb) {
                batch.add(key);
                if (batch.size() >= batchSize / 2) {
                    batch.forEach(map::remove);
                    db.commit();
                    removed += batch.size();
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            batch.forEach(map::remove);
            db.commit();
            removed += batch.size();
        }

        long durationMs = Duration.between(start, Instant.now()).toMillis();
        meterRegistry.timer("mapdb_clean_latency", "groupId", groupId.toString())
                .record(Duration.ofMillis(durationMs));
        meterRegistry.counter("mapdb_edges_cleaned", "groupId", groupId.toString()).increment(removed);
    }

    private Edge toEdge(GraphRecords.PotentialMatch match) {
        return GraphRequestFactory.toEdge(match);
    }

    private byte[] serializeMatch(GraphRecords.PotentialMatch match) {
        Kryo kryo = SerializerContext.get();
        try (Output output = new Output(64, 512)) {
            kryo.writeObject(output, match);
            return output.toBytes();
        } finally {
            SerializerContext.release();
        }
    }

    private GraphRecords.PotentialMatch deserializeMatch(byte[] data, UUID groupId, UUID domainId) {
        Kryo kryo = SerializerContext.get();
        try (Input input = new Input(data)) {
            GraphRecords.PotentialMatch match = kryo.readObject(input, GraphRecords.PotentialMatch.class);
            return new GraphRecords.PotentialMatch(
                    match.getReferenceId(),
                    match.getMatchedReferenceId(),
                    match.getCompatibilityScore(),
                    groupId,
                    domainId
            );
        } finally {
            SerializerContext.release();
        }
    }

    private byte[] compactKey(UUID groupId, int chunkIndex, GraphRecords.PotentialMatch match) {
        String refId = match.getReferenceId();
        String matchedId = match.getMatchedReferenceId();

        if (refId == null || matchedId == null) {
            throw new IllegalArgumentException("referenceId and matchedReferenceId must not be null");
        }

        byte[] refBytes = refId.getBytes(StandardCharsets.UTF_8);
        byte[] matchedBytes = matchedId.getBytes(StandardCharsets.UTF_8);

        int refLen = refBytes.length;
        int matchedLen = matchedBytes.length;

        if (refLen > MAX_STRING_LENGTH || matchedLen > MAX_STRING_LENGTH) {
            throw new IllegalArgumentException("String too long: max " + MAX_STRING_LENGTH + " bytes");
        }

        int totalLen = 8 + 8 + 4 + 2 + refLen + 2 + matchedLen;
        byte[] key = new byte[totalLen];
        int p = 0;

        putLong(key, p, groupId.getMostSignificantBits()); p += 8;
        putLong(key, p, groupId.getLeastSignificantBits()); p += 8;
        putInt(key, p, chunkIndex); p += 4;
        putShort(key, p, (short) refLen); p += 2;
        System.arraycopy(refBytes, 0, key, p, refLen); p += refLen;
        putShort(key, p, (short) matchedLen); p += 2;
        System.arraycopy(matchedBytes, 0, key, p, matchedLen);

        return key;
    }

    public Set<String> getKeysByGroupAndDomain(UUID groupId, UUID domainId) {
        Set<String> keys = new HashSet<>();
        long msb = groupId.getMostSignificantBits();
        long lsb = groupId.getLeastSignificantBits();

        for (HTreeMap.Entry<byte[], byte[]> entry : map.getEntries()) {
            byte[] key = entry.getKey();
            if (key.length >= 20 &&
                    getLong(key, 0) == msb &&
                    getLong(key, 8) == lsb) {
                try {
                    GraphRecords.PotentialMatch match = deserializeMatch(entry.getValue(), groupId, domainId);
                    if (domainId.equals(match.getDomainId())) {
                        keys.add(Base64.getEncoder().encodeToString(key));
                    }
                } catch (Exception e) {
                    log.warn("Skipping invalid match during key scan", e);
                } finally {
                    SerializerContext.release();
                }
            }
        }
        return keys;
    }

    private Set<UUID> safeDeserializeBucket(byte[] data, String key) {
        if (data == null || data.length == 0 || data.length > MAX_BUCKET_DATA_SIZE) {
            log.warn("Invalid bucket size: {} bytes for key {}", data == null ? 0 : data.length, key);
            return ConcurrentHashMap.newKeySet();
        }

        try {
            DataInput2 in = new DataInput2.ByteArray(data);

            if (data.length >= 8) {
                long timestamp = in.readLong();
                if (System.currentTimeMillis() - timestamp > BUCKET_TTL_MS) {
                    return ConcurrentHashMap.newKeySet();
                }

                int remaining = data.length - 8;
                if (remaining <= 0 || remaining > MAX_REMAINING_DATA_SIZE) {
                    log.warn("Invalid remaining data size: {} for key {}", remaining, key);
                    return ConcurrentHashMap.newKeySet();
                }

                @SuppressWarnings("unchecked")
                Set<UUID> set = (Set<UUID>) Serializer.JAVA.deserialize(in, remaining);
                if (set.size() > BUCKET_SIZE_LIMIT * 2) {
                    log.warn("Deserialized bucket too large: {} for key {}", set.size(), key);
                    return ConcurrentHashMap.newKeySet();
                }

                Set<UUID> copy = ConcurrentHashMap.newKeySet(set.size());
                copy.addAll(set);
                return copy;
            }

            @SuppressWarnings("unchecked")
            Set<UUID> set = (Set<UUID>) Serializer.JAVA.deserialize(in, data.length);

            if (set.size() > BUCKET_SIZE_LIMIT * 2) {
                log.warn("Old bucket too large: {} for key {}", set.size(), key);
                return ConcurrentHashMap.newKeySet();
            }

            Set<UUID> copy = ConcurrentHashMap.newKeySet(set.size());
            copy.addAll(set);

            mapdbExecutor.submit(() -> {
                try {
                    byte[] newData = serializeBucketWithTTL(copy, System.currentTimeMillis());
                    if (newData.length < MAX_BUCKET_DATA_SIZE) {
                        map.put(key.getBytes(StandardCharsets.UTF_8), newData);
                        db.commit();
                    }
                } catch (Exception e) {
                    log.warn("Failed to migrate old bucket {}", key, e);
                }
            });

            return copy;

        } catch (Exception e) {
            log.error("CRITICAL: Failed to deserialize bucket {} — possible corruption", key, e);
            return ConcurrentHashMap.newKeySet();
        }
    }

    @SuppressWarnings("unchecked")
    private byte[] serializeBucketWithTTL(Set<UUID> bucket, long timestamp) {
        DataOutput2 out = new DataOutput2();
        try {
            out.writeLong(timestamp);
            Serializer.JAVA.serialize(out, new HashSet<>(bucket));
            return out.copyBytes();
        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    public void addToBucket(int tableIdx, int band, Collection<UUID> nodeIds) {
        if (nodeIds.isEmpty() || nodeIds.size() > BUCKET_SIZE_LIMIT) return;

        byte[] key = getLSHKey(tableIdx, band);

        map.compute(key, (k, existingBytes) -> {
            Set<UUID> bucket = (existingBytes == null)
                    ? ConcurrentHashMap.newKeySet()
                    : safeDeserializeBucket(existingBytes, tableIdx, band);

            int added = 0;
            for (UUID id : nodeIds) {
                if (bucket.add(id)) added++;
            }

            if (bucket.size() > BUCKET_SIZE_LIMIT) {
                log.warn("LSH bucket overflow {}-{}: {} → trimming", tableIdx, band, bucket.size());
                bucket = bucket.stream()
                        .limit(BUCKET_TRIM_TO)
                        .collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
            }

            long total = totalBucketEntries.addAndGet(added);
            if (total > MAX_TOTAL_BUCKETS) {
                log.error("Global LSH limit exceeded: {} > {}", total, MAX_TOTAL_BUCKETS);
                totalBucketEntries.addAndGet(-added);
                return existingBytes;
            }

            return serializeBucketWithTTL(bucket, System.currentTimeMillis());
        });

        if (pendingCommits.incrementAndGet() >= 100) {
            db.commit();
            pendingCommits.set(0);
        }
    }
    public Set<UUID> getBucket(int tableIdx, int band) {
        byte[] key = getLSHKey(tableIdx, band);
        byte[] data = map.get(key);
        if (data == null) return Collections.emptySet();
        return safeDeserializeBucket(data, tableIdx, band);
    }

    public void clearAllBuckets() {
        Iterator<HTreeMap.Entry<byte[], byte[]>> iter = map.getEntries().iterator();
        List<byte[]> toRemove = new ArrayList<>();
        byte[] magicPrefix = new byte[4];
        putInt(magicPrefix, 0, LSH_MAGIC);

        while (iter.hasNext()) {
            byte[] key = iter.next().getKey();
            if (key.length == 8 && Arrays.equals(Arrays.copyOf(key, 4), magicPrefix)) {
                toRemove.add(key);
            }
        }
        toRemove.forEach(map::remove);
        db.commit();
        totalBucketEntries.set(0);
        log.info("Cleared all LSH buckets ({} removed)", toRemove.size());
    }

    private Set<UUID> safeDeserializeBucket(byte[] data, int tableIdx, int band) {
        if (data == null || data.length == 0 || data.length > MAX_BUCKET_DATA_SIZE) {
            log.warn("Invalid LSH bucket size: {} bytes for {}-{}",
                    data == null ? 0 : data.length, tableIdx, band);
            return ConcurrentHashMap.newKeySet();
        }

        try {
            DataInput2 in = new DataInput2.ByteArray(data);

            if (data.length >= 8) {
                long timestamp = in.readLong();
                if (System.currentTimeMillis() - timestamp > BUCKET_TTL_MS) {
                    return ConcurrentHashMap.newKeySet();
                }

                int remaining = data.length - 8;
                if (remaining <= 0 || remaining > MAX_REMAINING_DATA_SIZE) {
                    log.warn("Invalid remaining data: {} bytes for LSH {}-{}", remaining, tableIdx, band);
                    return ConcurrentHashMap.newKeySet();
                }

                @SuppressWarnings("unchecked")
                Set<UUID> set = (Set<UUID>) Serializer.JAVA.deserialize(in, remaining);
                if (set.size() > BUCKET_SIZE_LIMIT * 2) {
                    log.warn("Deserialized LSH bucket too large: {} for {}-{}", set.size(), tableIdx, band);
                    return ConcurrentHashMap.newKeySet();
                }

                Set<UUID> copy = ConcurrentHashMap.newKeySet(set.size());
                copy.addAll(set);
                return copy;
            }

            @SuppressWarnings("unchecked")
            Set<UUID> set = (Set<UUID>) Serializer.JAVA.deserialize(in, data.length);

            if (set.size() > BUCKET_SIZE_LIMIT * 2) {
                log.warn("Old LSH bucket too large: {} for {}-{}", set.size(), tableIdx, band);
                return ConcurrentHashMap.newKeySet();
            }

            Set<UUID> copy = ConcurrentHashMap.newKeySet(set.size());
            copy.addAll(set);

            mapdbExecutor.submit(() -> {
                try {
                    byte[] newData = serializeBucketWithTTL(copy, System.currentTimeMillis());
                    if (newData.length < MAX_BUCKET_DATA_SIZE) {
                        map.put(getLSHKey(tableIdx, band), newData);
                        db.commit();
                    }
                } catch (Exception e) {
                    log.warn("Failed to migrate LSH bucket {}-{}", tableIdx, band, e);
                }
            });

            return copy;

        } catch (Exception e) {
            log.error("CRITICAL: Failed to deserialize LSH bucket {}-{} — possible corruption", tableIdx, band, e);
            return ConcurrentHashMap.newKeySet();
        }
    }
}