package com.shedule.x.processors;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.shedule.x.config.factory.SerializerContext;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.exceptions.InternalServerErrorException;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.File;
import java.io.IOException;


@Slf4j
@Component
public class GraphStore implements AutoCloseable {
    private DB db;
    private HTreeMap<String, byte[]> map;
    private final ExecutorService mapdbExecutor;
    private final ExecutorService commitExecutor;
    private final ExecutorService cleanupExecutor;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger pendingCommits = new AtomicInteger(0);

    private final String dbPath;
    private final int batchSize;

    public GraphStore(
            @Qualifier("persistenceExecutor") ExecutorService mapdbExecutor,
            MeterRegistry meterRegistry,
            @Value("${mapdb.path:d:/web_dev/schedulex/graphstore}") String dbPath,
            @Value("${mapdb.batch-size:500}") int batchSize,
            @Value("${mapdb.commit-queue-max:1}") int commitQueueMax,
            @Value("${mapdb.commit-threads:2}") int commitThreads
    ) {
        this.mapdbExecutor = mapdbExecutor;
        this.meterRegistry = meterRegistry;
        this.dbPath = dbPath;
        this.batchSize = batchSize;

        this.commitExecutor = Executors.newFixedThreadPool(commitThreads, r -> {
            Thread t = new Thread(r, "mapdb-commit");
            t.setDaemon(true);
            return t;
        });

        this.cleanupExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "mapdb-cleanup");
            t.setDaemon(true);
            return t;
        });
    }

    @PostConstruct
    public void init() {
        try {
            if (dbPath == null || dbPath.trim().isEmpty()) {
                throw new IllegalArgumentException("mapdb.path is null or empty");
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
                    .keySerializer(Serializer.STRING)
                    .valueSerializer(Serializer.BYTE_ARRAY)
                    .createOrOpen();

            meterRegistry.gauge("mapdb_map_size", map, m -> (long) m.size());
            meterRegistry.gauge("mapdb_executor_queue", mapdbExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
            meterRegistry.gauge("mapdb_executor_active", mapdbExecutor, exec -> ((ThreadPoolExecutor) exec).getActiveCount());
            meterRegistry.gauge("mapdb_commit_executor_queue", commitExecutor, exec -> ((ThreadPoolExecutor) exec).getQueue().size());
            meterRegistry.gauge("mapdb_pending_commits", pendingCommits, AtomicInteger::get);

            new JvmMemoryMetrics().bindTo(meterRegistry);
            new JvmGcMetrics().bindTo(meterRegistry);

            log.info("Initialized MapDB at path={}", dbFile.getAbsolutePath());
        } catch (Exception e) {
            log.error("Failed to initialize MapDB at path={}: {}", dbPath, e.getMessage(), e);
            throw new InternalServerErrorException("Failed to initialize MapDB");
        }
    }

    public CompletableFuture<Void> persistEdgesAsyncFallback(List<GraphRecords.PotentialMatch> matches, String groupId, int chunkIndex, Throwable t) {
        log.warn("Persist failed for groupId={}, chunkIndex={}: {}", groupId, chunkIndex, t.getMessage());
        meterRegistry.counter("mapdb_persist_fallbacks", "groupId", groupId).increment(matches.size());
        return CompletableFuture.completedFuture(null);
    }

    public AutoCloseableStream<Edge> streamEdgesFallback(UUID domainId, UUID groupId, int topK, Throwable t) {
        log.warn("Stream failed for groupId={}: {}", groupId, t.getMessage());
        meterRegistry.counter("mapdb_stream_fallbacks", "groupId", groupId.toString()).increment();
        return new AutoCloseableStream<>(Stream.empty());
    }

    public void cleanEdgesFallback(UUID groupId, Throwable t) {
        log.warn("Clean failed for groupId={}: {}", groupId, t.getMessage());
        meterRegistry.counter("mapdb_clean_fallbacks", "groupId", groupId.toString()).increment();
    }

    public List<UUID> listGroupIds() {
        try {
            return map.keySet().stream()
                    .map(key -> key.split(":")[0])
                    .distinct()
                    .map(UUID::fromString)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to list groupIds: {}", e.getMessage(), e);
            throw new InternalServerErrorException("Failed to list groupIds");
        }
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            cleanupExecutor.shutdown();
            commitExecutor.shutdown();
            mapdbExecutor.shutdown();
            if (!cleanupExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
            if (!commitExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                commitExecutor.shutdownNow();
            }
            if (!mapdbExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                mapdbExecutor.shutdownNow();
            }
            if (map != null && !map.isClosed()) {
                map.close();
            }
            if (db != null && !db.isClosed()) {
                db.close();
            }
            log.info("Closed MapDB");
        } catch (Exception e) {
            log.error("Failed to close MapDB: {}", e.getMessage());
        }
    }

    private Edge toEdge(GraphRecords.PotentialMatch match) {
        return GraphRequestFactory.toEdge(match);
    }

    public CompletableFuture<Void> persistEdgesAsync(List<GraphRecords.PotentialMatch> matches, UUID groupId, int chunkIndex) {
        if (matches.isEmpty()) {
            log.debug("No matches to persist for groupId={}, chunkIndex={}", groupId, chunkIndex);
            return CompletableFuture.completedFuture(null);
        }

        Instant submitStart = Instant.now();
        int dynamicBatchSize = adjustBatchSize(matches.size());
        List<CompletableFuture<Void>> batchFutures = new ArrayList<>();
        for (List<GraphRecords.PotentialMatch> chunk : ListUtils.partition(matches, dynamicBatchSize)) {
            batchFutures.add(CompletableFuture.runAsync(() -> persistBatchAsync(chunk, groupId, chunkIndex), mapdbExecutor)
                    .orTimeout(1000, TimeUnit.SECONDS)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            log.error("Failed to persist batch for groupId={}, chunkIndex={}: {}",
                                    groupId, chunkIndex, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(), e);
                            meterRegistry.counter("mapdb_persist_batch_errors", "groupId", groupId.toString()).increment(chunk.size());
                        }
                    }));
        }

        return CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]))
                .thenRunAsync(() -> {
                    try {
                        Instant commitStart = Instant.now();
                        db.commit();
                        pendingCommits.set(0);
                        long durationMs = Duration.between(commitStart, Instant.now()).toMillis();
                        meterRegistry.timer("mapdb_commit_latency", "groupId", groupId.toString()).record(Duration.ofMillis(durationMs));
                        meterRegistry.counter("mapdb_commits", "groupId", groupId.toString()).increment();
                        log.info("Committed {} edges for groupId={}, chunkIndex={} in {} ms",
                                matches.size(), groupId, chunkIndex, durationMs);
                        if (durationMs > 1000) {
                            log.warn("Commit for groupId={}, chunkIndex={} took {} ms", groupId, chunkIndex, durationMs);
                        }
                    } catch (Exception e) {
                        log.error("Failed to commit for groupId={}, chunkIndex={}: {}",
                                groupId, chunkIndex, e.getMessage(), e);
                        meterRegistry.counter("mapdb_commit_errors", "groupId", groupId.toString()).increment();
                        throw new InternalServerErrorException("Failed to commit batch");
                    }
                }, commitExecutor)
                .whenComplete((v, e) -> {
                    meterRegistry.timer("mapdb_persist_submit_latency", "groupId", groupId.toString())
                            .record(Duration.between(submitStart, Instant.now()));
                    if (e != null) {
                        log.error("Failed to persist edges for groupId={}, chunkIndex={}: {}",
                                groupId, chunkIndex, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(), e);
                        meterRegistry.counter("mapdb_persist_errors", "groupId", groupId.toString()).increment(matches.size());
                    } else {
                        meterRegistry.counter("mapdb_edges_persisted", "groupId", groupId.toString()).increment(matches.size());
                    }
                });
    }

    private int adjustBatchSize(int totalEdges) {
        return Math.min(batchSize, totalEdges > 100_000 ? batchSize / 2 : batchSize);
    }

    private void persistBatchAsync(List<GraphRecords.PotentialMatch> subBatch, UUID groupId, int chunkIndex) {
        try {
            Instant writeStart = Instant.now();
            for (GraphRecords.PotentialMatch match : subBatch) {
                String key = compactKey(groupId, chunkIndex, match);
                map.put(key, serializeMatch(match));
            }
            long durationMs = Duration.between(writeStart, Instant.now()).toMillis();
            meterRegistry.timer("mapdb_persist_latency", "groupId", groupId.toString()).record(Duration.ofMillis(durationMs));
            meterRegistry.counter("mapdb_edges_persisted", "groupId", groupId.toString()).increment(subBatch.size());
            log.debug("Persisted {} edges for groupId={}, chunkIndex={} in {} ms",
                    subBatch.size(), groupId, chunkIndex, durationMs);
            if (durationMs > 1000) {
                log.warn("Batch persist of {} edges for groupId={}, chunkIndex={} took {} ms",
                        subBatch.size(), groupId, chunkIndex, durationMs);
            }
        } catch (Exception e) {
            log.error("Failed to persist batch for groupId={}, chunkIndex={}: {}",
                    groupId, chunkIndex, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(), e);
            meterRegistry.counter("mapdb_persist_errors", "groupId", groupId.toString()).increment(subBatch.size());
            throw new InternalServerErrorException("Failed to persist batch to MapDB");
        }
    }

    public AutoCloseableStream<Edge> streamEdges(UUID domainId, UUID groupId) {
        if (domainId == null) {
            log.error("Invalid domainId for groupId={}", groupId);
            throw new IllegalArgumentException("domainId must not be null");
        }

        Instant streamStart = Instant.now();
        Stream<Edge> stream = map.keySet().stream()
                .filter(k -> k != null && k.startsWith(groupId + ":"))
                .map(key -> {
                    byte[] data = map.get(key);
                    if (data != null) {
                        try {
                            GraphRecords.PotentialMatch match = deserializeMatch(data, groupId, domainId);
                            meterRegistry.counter("mapdb_edges_streamed", "groupId", groupId.toString()).increment();
                            return toEdge(match);
                        } catch (Exception e) {
                            log.error("Failed to deserialize match for key={} in groupId={}: {}",
                                    key, groupId, e.getMessage(), e);
                            meterRegistry.counter("mapdb_deserialize_errors", "groupId", groupId.toString()).increment();
                            return null;
                        } finally {
                            SerializerContext.release();
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .onClose(() -> {
                    long durationMs = Duration.between(streamStart, Instant.now()).toMillis();
                    meterRegistry.timer("mapdb_stream_latency", "groupId", groupId.toString()).record(Duration.ofMillis(durationMs));
                    log.info("Streamed edges for groupId={} in {} ms", groupId, durationMs);
                });

        return new AutoCloseableStream<>(stream);
    }

    public void cleanEdges(UUID groupId) {
        try {
            Instant cleanStart = Instant.now();
            Iterator<String> keyIterator = map.keySet().iterator();
            List<String> batch = new ArrayList<>(batchSize / 2);
            int removedCount = 0;

            while (keyIterator.hasNext()) {
                String key = keyIterator.next();
                if (key != null && key.startsWith(groupId + ":")) {
                    batch.add(key);
                    if (batch.size() >= batchSize / 2) {
                        batch.forEach(map::remove);
                        db.commit();
                        removedCount += batch.size();
                        batch.clear();
                    }
                }
            }

            if (!batch.isEmpty()) {
                batch.forEach(map::remove);
                db.commit();
                removedCount += batch.size();
            }

            long durationMs = Duration.between(cleanStart, Instant.now()).toMillis();
            meterRegistry.timer("mapdb_clean_latency", "groupId", groupId.toString()).record(Duration.ofMillis(durationMs));
            meterRegistry.counter("mapdb_edges_cleaned", "groupId", groupId.toString()).increment(removedCount);
            if (durationMs > 1000) {
                log.warn("Cleaning {} edges for groupId={} took {} ms", removedCount, groupId, durationMs);
            }
        } catch (Exception e) {
            log.error("Clean failed for groupId={}: {}", groupId, e.getMessage(), e);
            meterRegistry.counter("mapdb_clean_errors", "groupId", groupId.toString()).increment();
            throw new InternalServerErrorException("Failed to clean edges in MapDB");
        }
    }

    private byte[] serializeMatch(GraphRecords.PotentialMatch match) {
        Kryo kryo = SerializerContext.get();
        try (Output output = new Output(64, 512)) {
            kryo.writeObject(output, match);
            return output.toBytes();
        } catch (Exception e) {
            log.error("Serialization failed: referenceId={}, matchedReferenceId={}, cause={}",
                    match.getReferenceId(), match.getMatchedReferenceId(), e.getMessage(), e);
            throw new InternalServerErrorException("Failed to serialize PotentialMatch");
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
        } catch (Exception e) {
            log.error("Deserialization failed: groupId={}, domainId={}, cause={}", groupId, domainId, e.getMessage(), e);
            throw new InternalServerErrorException("Failed to deserialize PotentialMatch");
        } finally {
            SerializerContext.release();
        }
    }

    public Set<String> getKeysByGroupAndDomain(UUID groupId, UUID domainId) {
        return map.keySet().stream()
                .filter(k -> k.startsWith(groupId + ":"))
                .filter(k -> {
                    byte[] value = map.get(k);
                    if (value == null) return false;
                    try {
                        GraphRecords.PotentialMatch match = deserializeMatch(value, groupId, domainId);
                        return domainId.equals(match.getDomainId());
                    } catch (Exception e) {
                        log.warn("Skipping invalid match for key={}: {}", k, e.getMessage());
                        return false;
                    } finally {
                        SerializerContext.release();
                    }
                })
                .collect(Collectors.toSet());
    }

    private String compactKey(UUID groupId, int chunkIndex, GraphRecords.PotentialMatch match) {
        return groupId.toString() + ":" + chunkIndex + ":" + match.getReferenceId() + ":" + match.getMatchedReferenceId();
    }
}