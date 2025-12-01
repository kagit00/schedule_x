package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.service.GraphRecords;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Txn;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Component
@Slf4j
public class GraphStore implements AutoCloseable {
    private final EdgePersistence edgePersistence;
    private final LshBucketManager lshBucketManager;
    private final LmdbEnvironment lmdbEnv;

    public GraphStore(EdgePersistence edgePersistence,
                      LshBucketManager lshBucketManager,
                      LmdbEnvironment lmdbEnv) {
        this.edgePersistence = edgePersistence;
        this.lshBucketManager = lshBucketManager;
        this.lmdbEnv = lmdbEnv;
    }

    // --- Edge operations ---
    public CompletableFuture<Void> persistEdgesAsync(List<GraphRecords.PotentialMatch> matches, UUID groupId, int chunkIndex, String cycleId) {
        return edgePersistence.persistAsync(matches, groupId, chunkIndex, cycleId);
    }

    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId, String cycleId) {
        return edgePersistence.streamEdges(domainId, groupId, cycleId);
    }

    public void cleanEdges(UUID groupId, String cycleId) {
        edgePersistence.cleanEdges(groupId, cycleId);
    }

    public void bulkIngestLSH(Map<Integer, List<UUID>> groupedUpdates) {
        try (Txn<ByteBuffer> txn = lmdbEnv.env().txnWrite()) {

            for (Map.Entry<Integer, List<UUID>> entry : groupedUpdates.entrySet()) {
                int compositeKey = entry.getKey();
                int tableIdx = compositeKey >>> 16;
                int band = compositeKey & 0xFFFF;
                List<UUID> newNodes = entry.getValue();

                lshBucketManager.mergeAndWriteBucket(txn, tableIdx, band, newNodes);
            }

            txn.commit();
        } catch (Exception e) {
            log.error("Bulk LSH Ingest Failed. Transaction aborted.", e);
            throw new CompletionException(e);
        }
    }

    // --- LSH Bucket operations ---
    public Set<UUID> getBucket(int tableIdx, int band)          { return lshBucketManager.getBucket(tableIdx, band); }
    public int getBucketSize(int tableIdx, int band)            { return lshBucketManager.getBucketSize(tableIdx, band); }
    public void addToBucket(int tableIdx, int band, Collection<UUID> nodeIds) { lshBucketManager.addToBucket(tableIdx, band, nodeIds); }
    public void removeFromBucket(int tableIdx, int band, UUID nodeId)        { lshBucketManager.removeFromBucket(tableIdx, band, nodeId); }
    public void trimBucket(int tableIdx, int band, long targetSize)          { lshBucketManager.trimBucket(tableIdx, band, targetSize); }
    public void clearAllBuckets() { lshBucketManager.clearAllBuckets(); }

    @PreDestroy
    @Override
    public void close() {
        lmdbEnv.close();
    }
}