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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class LSHIndexImpl implements LSHIndex {
    private static final int BATCH_CHUNK_SIZE_INSERT = 200;
    private static final int MAX_CACHE_SIZE = 100_000;
    private static final int BUCKET_SIZE_LIMIT = 100_000;
    private static final double QUERY_METRICS_SAMPLE_RATE = 0.01;

    private final int numHashTables;
    private final int numBands;
    private final int topK;
    private final AtomicReference<List<ConcurrentHashMap<Integer, Set<UUID>>>> hashTables;
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

    public LSHIndexImpl(LSHConfig config, MeterRegistry meterRegistry, ExecutorService executor) {
        this.numHashTables = config.getNumHashTables();
        this.numBands = config.getNumBands();
        this.topK = config.getTopK();
        this.meterRegistry = meterRegistry != null ? meterRegistry : new SimpleMeterRegistry();
        this.executor = executor;
        this.hashTables = new AtomicReference<>(initializeHashTables());
        this.nodeHashCache = initializeCache();
        this.candidateCountSummary = initializeCandidateCountSummary();
        this.hashCollisionCounter = initializeHashCollisionCounter();
        this.updatesPool = initializeUpdatesPool();
        this.hashBufferPool = initializeHashBufferPool();

        registerMetrics();
        log.info("Initialized LSHIndex with {} hash tables, {} bands, topK={}", numHashTables, numBands, topK);
    }

    private List<ConcurrentHashMap<Integer, Set<UUID>>> initializeHashTables() {
        List<ConcurrentHashMap<Integer, Set<UUID>>> tables = new ArrayList<>(numHashTables);
        for (int i = 0; i < numHashTables; i++) {
            tables.add(new ConcurrentHashMap<>());
        }
        return tables;
    }

    private Cache<UUID, short[]> initializeCache() {
        return Caffeine.newBuilder().maximumSize(MAX_CACHE_SIZE).build();
    }

    private DistributionSummary initializeCandidateCountSummary() {
        return DistributionSummary.builder("lsh_query_candidate_count")
                .publishPercentiles(0.5, 0.95)
                .register(meterRegistry);
    }

    private Counter initializeHashCollisionCounter() {
        return Counter.builder("lsh_hash_collisions").register(meterRegistry);
    }

    private ThreadLocal<List<UUID>[]> initializeUpdatesPool() {
        return ThreadLocal.withInitial(() -> {
            @SuppressWarnings("unchecked")
            List<UUID>[] arr = new List[numHashTables * numBands];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = new ArrayList<>();
            }
            return arr;
        });
    }

    private ThreadLocal<short[]> initializeHashBufferPool() {
        return ThreadLocal.withInitial(() -> new short[numHashTables]);
    }

    private void registerMetrics() {
        meterRegistry.gauge("lsh_index_building", this, lsh -> lsh.isBuilding ? 1.0 : 0.0);
        meterRegistry.gauge("lsh_largest_bucket_size", this, lsh -> {
            long maxSize = 0;
            for (ConcurrentHashMap<Integer, Set<UUID>> table : hashTables.get()) {
                for (Set<UUID> bucket : table.values()) {
                    maxSize = Math.max(maxSize, bucket.size());
                }
            }
            return (double) maxSize;
        });
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
        }
    }

    @Override
    public CompletableFuture<Void> insertBatch(List<Map.Entry<int[], UUID>> entries) {
        if (entries == null || entries.isEmpty()) {
            log.warn("Empty or null entries list for batch insert");
            return CompletableFuture.completedFuture(null);
        }
        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        isBuilding = true;

        List<CompletableFuture<Void>> futures = BatchUtils.partition(entries, BATCH_CHUNK_SIZE_INSERT)
                .stream()
                .map(chunk -> CompletableFuture.runAsync(() -> processChunk(chunk, hashTables.get(), nodeHashCache, totalEntries), executor))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    isBuilding = false;
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(meterRegistry.timer("lsh_insert_duration")));
                    meterRegistry.counter("lsh_insert_total").increment(entries.size());
                    log.info("Inserted {} entries, total buckets: {}, duration: {}ms", entries.size(), totalEntries.get(), durationMs);
                })
                .exceptionally(ex -> {
                    isBuilding = false;
                    meterRegistry.counter("lsh_insert_errors").increment();
                    log.error("Batch insert failed", ex);
                    throw new CompletionException("LSH batch insert failed", ex);
                });
    }

    @Override
    public CompletableFuture<Void> prepareAsync(List<Map.Entry<int[], UUID>> entries) {
        if (entries == null || entries.isEmpty()) {
            log.warn("Empty or null entries list for prepareAsync");
            return CompletableFuture.completedFuture(null);
        }
        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        isBuilding = true;

        List<ConcurrentHashMap<Integer, Set<UUID>>> newTables = initializeHashTables();
        Cache<UUID, short[]> newHashCache = initializeCache();
        AtomicLong newTotalEntries = new AtomicLong();

        List<CompletableFuture<Void>> futures = BatchUtils.partition(entries, BATCH_CHUNK_SIZE_INSERT)
                .stream()
                .map(chunk -> CompletableFuture.runAsync(() -> processChunk(chunk, newTables, newHashCache, newTotalEntries), executor))
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    hashTables.set(newTables);
                    nodeHashCache.invalidateAll();
                    nodeHashCache.putAll(newHashCache.asMap());
                    totalEntries.set(newTotalEntries.get());
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(meterRegistry.timer("lsh_prepare_duration")));
                    log.info("Swapped to new LSH index with {} entries in {} ms", entries.size(), durationMs);
                })
                .whenComplete((v, e) -> isBuilding = false)
                .exceptionally(ex -> {
                    meterRegistry.counter("lsh_prepare_errors").increment();
                    log.error("Async prepare failed", ex);
                    throw new CompletionException("LSH async prepare failed", ex);
                });
    }

    private void processChunk(List<Map.Entry<int[], UUID>> chunk, List<ConcurrentHashMap<Integer, Set<UUID>>> tables,
                              Cache<UUID, short[]> hashCache, AtomicLong entriesCounter) {
        List<UUID>[] updates = updatesPool.get();
        Arrays.stream(updates).forEach(List::clear);
        long chunkEntryCount = 0;

        for (Map.Entry<int[], UUID> entry : chunk) {
            UUID nodeId = entry.getValue();
            int[] metadata = entry.getKey();
            if (nodeId == null || metadata == null || metadata.length == 0) {
                log.warn("Skipping invalid entry: nodeId={}, metadata={}", nodeId, metadata);
                continue;
            }

            short[] hashes = hashBufferPool.get();
            for (int i = 0; i < numHashTables; i++) {
                hashes[i] = (short) HashUtils.computeHash(metadata, i, numBands);
            }
            hashCache.put(nodeId, Arrays.copyOf(hashes, numHashTables));

            for (int i = 0; i < numHashTables; i++) {
                updates[i * numBands + hashes[i]].add(nodeId);
            }
        }

        for (int tableIdx = 0; tableIdx < numHashTables; tableIdx++) {
            ConcurrentHashMap<Integer, Set<UUID>> table = tables.get(tableIdx);
            for (int hash = 0; hash < numBands; hash++) {
                int compositeKeyIndex = tableIdx * numBands + hash;
                List<UUID> uuids = updates[compositeKeyIndex];
                if (!uuids.isEmpty()) {
                    table.compute(hash, (k, bucket) -> {
                        bucket = bucket == null ? ConcurrentHashMap.newKeySet() : bucket;
                        bucket.addAll(uuids);
                        if (bucket.size() > BUCKET_SIZE_LIMIT * 0.8) {
                            log.warn("Large bucket detected: size={}", bucket.size());
                        }
                        return bucket;
                    });
                    chunkEntryCount += uuids.size();
                }
            }
        }
        entriesCounter.addAndGet(chunkEntryCount);
    }

    @Override
    public CompletableFuture<Void> insertSingle(int[] metadata, UUID nodeId) {
        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        return CompletableFuture.runAsync(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            try {
                if (metadata == null || nodeId == null) {
                    log.warn("Null metadata or nodeId: metadata={}, nodeId={}", metadata, nodeId);
                    return;
                }
                short[] hashes = HashUtils.computeHashes(metadata, numHashTables, numBands);
                nodeHashCache.put(nodeId, hashes);

                List<ConcurrentHashMap<Integer, Set<UUID>>> currentTables = hashTables.get();
                for (int i = 0; i < numHashTables; i++) {
                    int hash = hashes[i];
                    currentTables.get(i).compute(hash, (k, bucket) -> {
                        bucket = bucket == null ? ConcurrentHashMap.newKeySet() : bucket;
                        if (!bucket.add(nodeId)) {
                            hashCollisionCounter.increment();
                        }
                        return bucket;
                    });
                }
                totalEntries.incrementAndGet();
                sample.stop(meterRegistry.timer("lsh_insert_duration"));
            } catch (Exception e) {
                meterRegistry.counter("lsh_insert_errors").increment();
                log.error("Single insert failed for node {}: {}", nodeId, e.getMessage());
                throw new CompletionException("LSH single insert failed", e);
            }
        }, executor);
    }

    @Override
    public Set<UUID> querySync(int[] metadata, UUID nodeId) {
        boolean shouldSample = ThreadLocalRandom.current().nextDouble() < QUERY_METRICS_SAMPLE_RATE;
        Timer.Sample sample = shouldSample ? Timer.start(meterRegistry) : null;
        Set<UUID> candidates = ConcurrentHashMap.newKeySet();

        try {
            if (metadata == null) {
                log.warn("Null metadata for query: nodeId={}", nodeId);
                return Collections.emptySet();
            }

            short[] hashes = nodeHashCache.getIfPresent(nodeId);
            if (hashes == null) {
                hashes = HashUtils.computeHashes(metadata, numHashTables, numBands);
                nodeHashCache.put(nodeId, hashes);
            }

            List<ConcurrentHashMap<Integer, Set<UUID>>> currentTables = hashTables.get();
            for (int i = 0; i < numHashTables; i++) {
                candidates.addAll(currentTables.get(i).getOrDefault((int) hashes[i], Collections.emptySet()));
            }
        } finally {
            if (shouldSample && sample != null) {
                sample.stop(meterRegistry.timer("lsh_query_duration_sampled"));
                meterRegistry.counter("lsh_query_candidates_total_sampled").increment(candidates.size());
                candidateCountSummary.record(candidates.size());
            }
            queryCounter.incrementAndGet();
        }
        return candidates;
    }

    @Override
    public CompletableFuture<Set<UUID>> queryAsync(int[] metadata, UUID nodeId) {
        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }
        return CompletableFuture.supplyAsync(() -> querySync(metadata, nodeId), executor)
                .exceptionally(ex -> {
                    log.error("Query task failed", ex);
                    throw new CompletionException("LSH query failed", ex);
                });
    }

    @Override
    public CompletableFuture<Map<UUID, Set<UUID>>> queryAsyncAll(List<Pair<int[], UUID>> nodeEncodings) {
        if (nodeEncodings == null || nodeEncodings.isEmpty()) {
            log.warn("Empty or null nodeEncodings list for bulk query");
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        if (shutdownInitiated) {
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        return CompletableFuture.supplyAsync(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            Map<UUID, Set<UUID>> allCandidates = new ConcurrentHashMap<>();

            nodeEncodings.parallelStream().forEach(pair -> {
                try {
                    allCandidates.put(pair.getValue(), querySync(pair.getKey(), pair.getValue()));
                } catch (Exception e) {
                    log.error("Error processing node {} in bulk query: {}", pair.getValue(), e.getMessage());
                    meterRegistry.counter("lsh_bulk_query_node_errors").increment();
                }
            });

            sample.stop(meterRegistry.timer("lsh_bulk_query_duration"));
            meterRegistry.counter("lsh_bulk_query_total_nodes").increment(nodeEncodings.size());
            return allCandidates;
        }, executor);
    }

    @Override
    public long totalBucketsCount() {
        return totalEntries.get();
    }

    @Override
    public boolean isBuilding() {
        return isBuilding;
    }

    @Override
    public void clean() {
        shutdownInitiated = false;
        isBuilding = false;

        List<ConcurrentHashMap<Integer, Set<UUID>>> freshTables = initializeHashTables();
        hashTables.set(freshTables);
        nodeHashCache.invalidateAll();
        totalEntries.set(0L);
        updatesPool.remove();
        hashBufferPool.remove();

        log.info("All in‐memory buckets, caches, and counters reset.");
    }
}