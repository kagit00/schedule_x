package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<Void> persistEdgesAsync(List<GraphRecords.PotentialMatch> matches, UUID groupId, int chunkIndex) {
        return edgePersistence.persistAsync(matches, groupId, chunkIndex);
    }

    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId) {
        return edgePersistence.streamEdges(domainId, groupId);
    }

    public void cleanEdges(UUID groupId) {
        edgePersistence.cleanEdges(groupId);
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