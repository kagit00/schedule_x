package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.MeterRegistry;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Txn;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class GraphStore implements AutoCloseable {

    private final EdgePersistence edgePersistence;
    private final LshBucketManager lshBucketManager;
    private final LmdbEnvironment lmdbEnv;
    private final MeterRegistry meters;

    private static final int LSH_BATCH_SIZE = 512;

    // --- Edge operations ---
    public CompletableFuture<Void> persistEdgesAsync(List<GraphRecords.PotentialMatch> matches,
                                                     UUID groupId, int chunkIndex, String cycleId) {
        return edgePersistence.persistAsync(matches, groupId, chunkIndex, cycleId);
    }

    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId, String cycleId) {
        return edgePersistence.streamEdges(domainId, groupId, cycleId);
    }

    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId) {
        return edgePersistence.streamEdges(domainId, groupId);
    }

    public void cleanEdges(UUID groupId, String cycleId) {
        edgePersistence.cleanEdges(groupId, cycleId);
    }

    public CompletableFuture<Void> bulkIngestLSH(Long2ObjectMap<List<UUID>> updates) {
        if (updates == null || updates.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final int totalBuckets = updates.size();
        meters.counter("lsh.bulk_ingest.requests").increment();
        meters.summary("lsh.bulk_ingest.bucket_count").record(totalBuckets);

        long startTime = System.nanoTime();
        log.debug("Enqueueing LSH bulk ingest for {} buckets", totalBuckets);

        List<CompletableFuture<Void>> chunkFutures = new ArrayList<>();
        List<Long> keys = new ArrayList<>(updates.keySet());

        int chunksCreated = 0;
        for (int i = 0; i < keys.size(); i += LSH_BATCH_SIZE) {
            int end = Math.min(keys.size(), i + LSH_BATCH_SIZE);

            Long2ObjectOpenHashMap<List<UUID>> chunk = new Long2ObjectOpenHashMap<>(end - i);
            for (int j = i; j < end; j++) {
                long key = keys.get(j);
                chunk.put(key, updates.get(key));
            }

            CompletableFuture<Void> chunkFuture = edgePersistence.enqueueLshChunk(chunk);
            chunkFutures.add(chunkFuture);
            chunksCreated++;
        }

        final int totalChunks = chunksCreated;
        meters.gauge("lsh.bulk_ingest.chunks_enqueued", totalChunks);

        return CompletableFuture.allOf(chunkFutures.toArray(CompletableFuture[]::new))
                .whenComplete((result, error) -> {
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

                    if (error != null) {
                        meters.counter("lsh.bulk_ingest.failure").increment();
                        meters.timer("lsh.bulk_ingest.duration", "status", "failed")
                                .record(durationMs, TimeUnit.MILLISECONDS);
                        log.error("LSH bulk ingest failed: {} buckets in {} chunks after {}ms",
                                totalBuckets, totalChunks, durationMs, error);
                    } else {
                        meters.counter("lsh.bulk_ingest.success").increment();
                        meters.timer("lsh.bulk_ingest.duration", "status", "success")
                                .record(durationMs, TimeUnit.MILLISECONDS);
                        log.info("LSH bulk ingest completed: {} buckets in {} chunks ({}ms)",
                                totalBuckets, totalChunks, durationMs);
                    }
                });
    }

    // --- LSH Bucket operations ---
    public Set<UUID> getBucket(int tableIdx, int band) {
        return lshBucketManager.getBucket(tableIdx, band);
    }

    public int getBucketSize(int tableIdx, int band) {
        return lshBucketManager.getBucketSize(tableIdx, band);
    }

    public void addToBucket(int tableIdx, int band, Collection<UUID> nodeIds) {
        lshBucketManager.addToBucket(tableIdx, band, nodeIds);
    }

    public void removeFromBucket(int tableIdx, int band, UUID nodeId) {
        lshBucketManager.removeFromBucket(tableIdx, band, nodeId);
    }

    public void trimBucket(int tableIdx, int band, long targetSize) {
        lshBucketManager.trimBucket(tableIdx, band, targetSize);
    }

    public void clearAllBuckets() {
        lshBucketManager.clearAllBuckets();
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            log.info("Shutting down GraphStore...");

            if (edgePersistence != null) {
                try {
                    edgePersistence.close();
                    log.debug("EdgePersistence closed successfully");
                } catch (Exception e) {
                    log.error("Error closing EdgePersistence", e);
                }
            }

            if (lmdbEnv != null) {
                try {
                    lmdbEnv.close();
                    log.debug("LMDB environment closed successfully");
                } catch (Exception e) {
                    log.error("Error closing LMDB environment", e);
                }
            }

            log.info("GraphStore shutdown complete");
        } catch (Exception e) {
            log.error("Unexpected error during GraphStore shutdown", e);
        }
    }
}