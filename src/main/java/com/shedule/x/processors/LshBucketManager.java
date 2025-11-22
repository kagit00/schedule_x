package com.shedule.x.processors;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

public interface LshBucketManager extends AutoCloseable {
    Set<UUID> getBucket(int tableIdx, int band);
    int getBucketSize(int tableIdx, int band);
    void addToBucket(int tableIdx, int band, Collection<UUID> nodeIds);
    void removeFromBucket(int tableIdx, int band, UUID nodeId);
    void trimBucket(int tableIdx, int band, long targetSize);
    void clearAllBuckets();
}