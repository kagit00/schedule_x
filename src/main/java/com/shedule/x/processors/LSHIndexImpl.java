package com.shedule.x.processors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.shedule.x.config.LSHConfig;
import com.shedule.x.utils.basic.HashUtils;
import com.shedule.x.utils.db.BatchUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;

import java.util.*;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import java.util.concurrent.ThreadLocalRandom;


import org.springframework.beans.factory.annotation.Qualifier;

import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
public class LSHIndexImpl implements LSHIndex {

    private static final int BATCH_CHUNK_SIZE_INSERT = 200;
    private static final int MAX_CACHE_SIZE = 100_000;
    private static final double QUERY_METRICS_SAMPLE_RATE = 0.01;
    private static final double BUCKET_TRIM_RATIO = 0.9;

    private final int numHashTables;
    private final int numBands;
    private final int topK;
    private final GraphStore graphStore;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger queryCounter = new AtomicInteger();
    private final DistributionSummary candidateCountSummary;
    private final Counter hashCollisionCounter;
    private final Cache<UUID, short[]> nodeHashCache;
    private final AtomicLong totalEntries = new AtomicLong();
    private final ExecutorService executor;
    private volatile boolean isBuilding = false;
    private volatile boolean shutdownInitiated = false;
    private final ThreadLocal<List<UUID>[]> updatesPool;
    private final ThreadLocal<short[]> hashBufferPool;
    private final ReentrantLock buildingLock = new ReentrantLock();

    public LSHIndexImpl(LSHConfig config,
                        MeterRegistry meterRegistry,
                        @Qualifier("indexExecutor") ExecutorService executor,
                        GraphStore graphStore) {
        this.numHashTables = config.getNumHashTables();
        this.numBands = config.getNumBands();
        this.topK = config.getTopK();
        this.meterRegistry = meterRegistry;
        this.graphStore = graphStore;
        this.executor = executor;
        this.nodeHashCache = initializeCache();
        this.candidateCountSummary = initializeCandidateCountSummary();
        this.hashCollisionCounter = initializeHashCollisionCounter();
        this.updatesPool = initializeUpdatesPool();
        this.hashBufferPool = initializeHashBufferPool();
        registerMetrics();
        log.info("Initialized LSHIndex with {} tables, {} bands, topK={}",
                numHashTables, numBands, topK);
    }

    private Cache<UUID, short[]> initializeCache() {
        return Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
    }

    private DistributionSummary initializeCandidateCountSummary() {
        return DistributionSummary.builder("lsh_query_candidate_count")
                .publishPercentiles(0.5, 0.95)
                .register(meterRegistry);
    }

    private Counter initializeHashCollisionCounter() {
        return Counter.builder("lsh_hash_collisions")
                .register(meterRegistry);
    }

    private ThreadLocal<List<UUID>[]> initializeUpdatesPool() {
        return ThreadLocal.withInitial(() -> {
            List<UUID>[] arr = new List[numHashTables * numBands];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = new ArrayList<>(16);
            }
            return arr;
        });
    }

    private ThreadLocal<short[]> initializeHashBufferPool() {
        return ThreadLocal.withInitial(() -> new short[numHashTables]);
    }

    private void registerMetrics() {
        meterRegistry.gauge("lsh_index_building", this, lsh -> lsh.isBuilding ? 1.0 : 0.0);
    }

    @PreDestroy
    public void shutdown() {
        shutdownInitiated = true;
        try {
            executor.shutdown();
            if (!executor.awaitTermination(20, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            cleanupThreadLocals();
        }
    }

    private void cleanupThreadLocals() {
        updatesPool.remove();
        hashBufferPool.remove();
    }

    @Override
    public CompletableFuture<Void> insertBatch(List<Map.Entry<int[], UUID>> entries) {
        if (entries == null || entries.isEmpty()) {
            log.warn("Empty batch insert");
            return CompletableFuture.completedFuture(null);
        }
        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shutting down"));
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        isBuilding = true;

        List<CompletableFuture<Void>> futures = BatchUtils.partition(entries, BATCH_CHUNK_SIZE_INSERT)
                .stream()
                .map(chunk -> CompletableFuture.runAsync(() -> processChunk(chunk), executor))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    isBuilding = false;
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(meterRegistry.timer("lsh_insert_duration")));
                    meterRegistry.counter("lsh_insert_total").increment(entries.size());
                    log.info("Inserted {} entries, total: {}, {}ms",
                            entries.size(), totalEntries.get(), durationMs);
                })
                .exceptionally(ex -> {
                    isBuilding = false;
                    meterRegistry.counter("lsh_insert_errors").increment();
                    log.error("Batch insert failed", ex);
                    throw new CompletionException(ex);
                });
    }

    private void processChunk(List<Map.Entry<int[], UUID>> chunk) {
        List<UUID>[] updates = updatesPool.get();
        Arrays.stream(updates).forEach(List::clear);
        long added = 0;

        for (Map.Entry<int[], UUID> e : chunk) {
            UUID nodeId = e.getValue();
            int[] metadata = e.getKey();
            if (nodeId == null || metadata == null || metadata.length == 0) continue;

            // Unified hash computation
            short[] newHashes = HashUtils.computeHashes(metadata, numHashTables, numBands);
            short[] oldHashes = nodeHashCache.getIfPresent(nodeId);

            // Handle duplicate nodes - remove from old buckets
            if (oldHashes != null) {
                for (int i = 0; i < numHashTables; i++) {
                    graphStore.removeFromBucket(i, oldHashes[i], nodeId);
                }
            }

            // Cache new hashes
            nodeHashCache.put(nodeId, Arrays.copyOf(newHashes, numHashTables));

            // Add to new buckets
            for (int i = 0; i < numHashTables; i++) {
                int bandIndex = newHashes[i] & 0xFFFF; // Ensure non-negative
                List<UUID> bucket = updates[i * numBands + bandIndex];
                if (!bucket.contains(nodeId)) {
                    bucket.add(nodeId);
                }
            }
        }

        for (int t = 0; t < numHashTables; t++) {
            for (int b = 0; b < numBands; b++) {
                List<UUID> list = updates[t * numBands + b];
                if (!list.isEmpty()) {
                    int before = graphStore.getBucket(t, b).size();
                    graphStore.addToBucket(t, b, list);
                    int after = graphStore.getBucket(t, b).size();
                    hashCollisionCounter.increment(after - before);
                    added += list.size();
                }
            }
        }
        totalEntries.addAndGet(added);
    }

    @Override
    public long getNodePriorityScore(UUID nodeId) {
        short[] hashes = nodeHashCache.getIfPresent(nodeId);
        if (hashes != null && hashes.length > 0) {
            return hashes[0] & 0xFFFFL;
        }
        return nodeId.getMostSignificantBits() ^ nodeId.getLeastSignificantBits();
    }

    @Override
    public Set<UUID> querySync(int[] metadata, UUID nodeId) {
        boolean sample = ThreadLocalRandom.current().nextDouble() < QUERY_METRICS_SAMPLE_RATE;
        Timer.Sample timer = sample ? Timer.start(meterRegistry) : null;
        Set<UUID> candidates = ConcurrentHashMap.newKeySet();

        try {
            if (metadata == null || nodeId == null) return Collections.emptySet();

            // Unified hash computation
            short[] hashes = nodeHashCache.getIfPresent(nodeId);
            if (hashes == null) {
                hashes = HashUtils.computeHashes(metadata, numHashTables, numBands);
                nodeHashCache.put(nodeId, hashes);
            }

            for (int i = 0; i < numHashTables; i++) {
                int bandIndex = hashes[i] & 0xFFFF;
                Set<UUID> bucket = graphStore.getBucket(i, bandIndex);
                candidates.addAll(bucket);
            }

            // Apply topK filtering
            if (candidates.size() > topK) {
                candidates = candidates.stream()
                        .limit(topK)
                        .collect(Collectors.toSet());
            }
        } catch (Exception e) {
            log.error("Query failed for node {}", nodeId, e);
            return Collections.emptySet();
        } finally {
            if (sample && timer != null) {
                timer.stop(meterRegistry.timer("lsh_query_duration_sampled"));
                candidateCountSummary.record(candidates.size());
            }
            queryCounter.incrementAndGet();
        }
        return candidates;
    }

    @Override
    public CompletableFuture<Set<UUID>> queryAsync(int[] metadata, UUID nodeId) {
        return CompletableFuture.supplyAsync(() -> querySync(metadata, nodeId), executor);
    }

    @Override
    public CompletableFuture<Map<UUID, Set<UUID>>> queryAsyncAll(List<Pair<int[], UUID>> nodes) {
        return CompletableFuture.supplyAsync(() -> {
            Timer.Sample s = Timer.start(meterRegistry);
            Map<UUID, Set<UUID>> result = new ConcurrentHashMap<>();
            nodes.parallelStream().forEach(p -> {
                result.put(p.getValue(), querySync(p.getKey(), p.getValue()));
            });
            s.stop(meterRegistry.timer("lsh_bulk_query_duration"));
            meterRegistry.counter("lsh_bulk_query_total_nodes").increment(nodes.size());
            return result;
        }, executor);
    }

    @Override
    public long totalBucketsCount() { return totalEntries.get(); }

    @Override
    public boolean isBuilding() {
        return buildingLock.isHeldByCurrentThread() || isBuilding;
    }

    @Override
    public void clean() {
        buildingLock.lock();
        try {
            graphStore.clearAllBuckets();
            nodeHashCache.invalidateAll();
            totalEntries.set(0L);
            log.info("LSH index cleared (persistent buckets removed)");
        } finally {
            buildingLock.unlock();
        }
    }

    @Override
    public void trimBuckets() {
        buildingLock.lock();
        try {
            for (int t = 0; t < numHashTables; t++) {
                for (int b = 0; b < numBands; b++) {
                    Set<UUID> bucket = graphStore.getBucket(t, b);
                    if (bucket.size() > BUCKET_TRIM_RATIO * MAX_CACHE_SIZE) {
                        graphStore.trimBucket(t, b, (long) (bucket.size() * BUCKET_TRIM_RATIO));
                    }
                }
            }
        } finally {
            buildingLock.unlock();
        }
    }

    @Override
    public void updateNode(UUID nodeId, int[] newMetadata) {
        buildingLock.lock();
        try {
            if (newMetadata == null) {
                throw new IllegalArgumentException("Metadata cannot be null");
            }

            // Remove old data
            short[] oldHashes = nodeHashCache.getIfPresent(nodeId);
            if (oldHashes != null) {
                for (int i = 0; i < numHashTables; i++) {
                    graphStore.removeFromBucket(i, oldHashes[i], nodeId);
                }
                nodeHashCache.invalidate(nodeId);
            }

            // Add new data
            short[] newHashes = HashUtils.computeHashes(newMetadata, numHashTables, numBands);
            nodeHashCache.put(nodeId, newHashes);
            for (int i = 0; i < numHashTables; i++) {
                int bandIndex = newHashes[i] & 0xFFFF;
                graphStore.addToBucket(i, bandIndex, Collections.singleton(nodeId));
            }
            totalEntries.addAndGet(numHashTables);
            log.debug("Updated node {}", nodeId);
        } finally {
            buildingLock.unlock();
        }
    }

    @Override
    public void removeNode(UUID nodeId) {
        buildingLock.lock();
        try {
            short[] hashes = nodeHashCache.getIfPresent(nodeId);
            if (hashes != null) {
                for (int i = 0; i < numHashTables; i++) {
                    graphStore.removeFromBucket(i, hashes[i], nodeId);
                }
                nodeHashCache.invalidate(nodeId);
                totalEntries.addAndGet((long) -numHashTables * numBands);
                log.debug("Removed node {}", nodeId);
            }
        } finally {
            buildingLock.unlock();
        }
    }
}