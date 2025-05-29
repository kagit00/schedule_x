package com.shedule.x.processors;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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

    private final int numHashTables;
    private final int numBands;
    private final AtomicReference<List<ConcurrentHashMap<Integer, Set<UUID>>>> hashTables;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger queryCounter = new AtomicInteger();
    private final DistributionSummary candidateCountSummary;
    private final Counter hashCollisionCounter;
    private final Caffeine<Object, Object> cacheBuilder;
    private final Cache<UUID, short[]> nodeHashCache;
    private final AtomicLong totalEntries = new AtomicLong();
    private final ExecutorService executor;
    private volatile boolean isBuilding = false;
    private volatile boolean shutdownInitiated = false;
    private final ThreadLocal<List<UUID>[]> updatesPool;
    private final ThreadLocal<short[]> hashBufferPool;
    private static final double QUERY_METRICS_SAMPLE_RATE = 0.01;
    private final int topK;

    public LSHIndexImpl(
            int numHashTables,
            int numBands,
            MeterRegistry meterRegistry,
            ExecutorService executor,
            int topK
    ) {
        this.numHashTables = numHashTables;
        this.numBands = numBands;
        this.meterRegistry = meterRegistry != null ? meterRegistry : new SimpleMeterRegistry();
        this.executor = executor;
        this.topK = topK;

        this.hashTables = new AtomicReference<>(new ArrayList<>(numHashTables));
        for (int i = 0; i < numHashTables; i++) {
            this.hashTables.get().add(new ConcurrentHashMap<>());
        }
        this.cacheBuilder = Caffeine.newBuilder().maximumSize(MAX_CACHE_SIZE);
        this.nodeHashCache = cacheBuilder.build();
        this.candidateCountSummary = DistributionSummary.builder("lsh_query_candidate_count")
                .publishPercentiles(0.5, 0.95)
                .register(this.meterRegistry);
        this.hashCollisionCounter = Counter.builder("lsh_hash_collisions")
                .register(this.meterRegistry);
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
        log.info("Initialized LSHIndex with {} hash tables, {} bands", numHashTables, numBands);

        this.updatesPool = ThreadLocal.withInitial(() -> {
            @SuppressWarnings("unchecked")
            List<UUID>[] arr = new List[numHashTables * numBands];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = new ArrayList<>();
            }
            return arr;
        });

        this.hashBufferPool = ThreadLocal.withInitial(() -> new short[numHashTables]);
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

    public CompletableFuture<Void> insertBatch(List<Map.Entry<int[], UUID>> entries) {
        if (entries == null || entries.isEmpty()) {
            log.warn("Empty or null entries list for batch insert");
            return CompletableFuture.completedFuture(null);
        }
        if (shutdownInitiated) {
            log.warn("Batch insert aborted due to shutdown");
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Inserting {} entries", entries.size());
        isBuilding = true;

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        if (entries.size() <= BATCH_CHUNK_SIZE_INSERT) {
            futures.add(CompletableFuture.runAsync(() -> processChunk(entries), executor));
        } else {
            for (int i = 0; i < entries.size(); i += BATCH_CHUNK_SIZE_INSERT) {
                final int startIdx = i;
                final int endIdx = Math.min(i + BATCH_CHUNK_SIZE_INSERT, entries.size());
                List<Map.Entry<int[], UUID>> chunk = entries.subList(startIdx, endIdx);
                futures.add(CompletableFuture.runAsync(() -> processChunk(chunk), executor));
            }
        }
        log.info("Processing {} insert tasks", futures.size());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    isBuilding = false;
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                            meterRegistry.timer("lsh_insert_duration")));
                    meterRegistry.counter("lsh_insert_total").increment(entries.size());
                    long bucketCount = totalBucketsCount();
                    log.info("Inserted {} entries, total buckets: {}, duration: {}ms", entries.size(), bucketCount, durationMs);
                    if (totalEntries.get() < entries.size()) {
                        log.warn("Incomplete insert: {}/{} entries inserted, possible duplicates or invalid entries",
                                totalEntries.get(), entries.size());
                    }
                })
                .exceptionally(ex -> {
                    isBuilding = false;
                    meterRegistry.counter("lsh_insert_errors").increment();
                    log.error("Batch insert failed: {}", ex.getMessage(), ex);
                    throw new CompletionException("LSH batch insert failed", ex);
                });
    }

    public CompletableFuture<Void> prepareAsync(List<Map.Entry<int[], UUID>> entries) {
        if (entries == null || entries.isEmpty()) {
            log.warn("Empty or null entries list for prepareAsync");
            return CompletableFuture.completedFuture(null);
        }
        if (shutdownInitiated) {
            log.warn("PrepareAsync aborted due to shutdown");
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        log.info("Starting async preparation with {} entries, lsh-executor queue={}, active={}",
                entries.size(), ((ThreadPoolExecutor) executor).getQueue().size(), ((ThreadPoolExecutor) executor).getActiveCount());
        Timer.Sample sample = Timer.start(meterRegistry);
        isBuilding = true;

        List<ConcurrentHashMap<Integer, Set<UUID>>> newTables = new ArrayList<>(numHashTables);
        for (int i = 0; i < numHashTables; i++) {
            newTables.add(new ConcurrentHashMap<>());
        }
        Cache<UUID, short[]> newHashCache = cacheBuilder.build();
        AtomicLong newTotalEntries = new AtomicLong();

        List<List<Map.Entry<int[], UUID>>> chunks = BatchUtils.partition(entries, BATCH_CHUNK_SIZE_INSERT);

        List<CompletableFuture<Void>> chunkFutures = chunks.stream()
                .filter(chunk -> chunk != null && !chunk.isEmpty())
                .map(chunk -> CompletableFuture.runAsync(
                        () -> processChunk(chunk, newTables, newHashCache, newTotalEntries), executor))
                .toList();

        return CompletableFuture.allOf(chunkFutures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    List<ConcurrentHashMap<Integer, Set<UUID>>> oldTables = hashTables.getAndSet(newTables);
                    this.nodeHashCache.invalidateAll();
                    this.nodeHashCache.putAll(newHashCache.asMap());
                    this.totalEntries.set(newTotalEntries.get());
                    oldTables.forEach(Map::clear);
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                            meterRegistry.timer("lsh_prepare_duration")));
                    log.info("Swapped to new LSH index with {} entries in {} ms", entries.size(), durationMs);
                })
                .whenComplete((v, e) -> isBuilding = false)
                .exceptionally(ex -> {
                    meterRegistry.counter("lsh_prepare_errors").increment();
                    log.error("Async prepare failed: {}", ex.getMessage(), ex);
                    throw new CompletionException("LSH async prepare failed", ex);
                });
    }

    private void processChunk(List<Map.Entry<int[], UUID>> chunk) {
        processChunk(chunk, hashTables.get(), nodeHashCache, totalEntries);
    }

    private void processChunk(List<Map.Entry<int[], UUID>> chunk, List<ConcurrentHashMap<Integer, Set<UUID>>> tables,
                              Cache<UUID, short[]> hashCache, AtomicLong entriesCounter) {
        List<UUID>[] updates = updatesPool.get();

        for (List<UUID> update : updates) {
            if (!update.isEmpty()) {
                update.clear();
            }
        }
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
                hashes[i] = (short) computeHash(metadata, i);
            }
            hashCache.put(nodeId, Arrays.copyOf(hashes, numHashTables));

            for (int i = 0; i < numHashTables; i++) {
                short hash = hashes[i];
                int compositeKeyIndex = i * numBands + hash;
                updates[compositeKeyIndex].add(nodeId);
            }
        }

        for (int tableIdx = 0; tableIdx < numHashTables; tableIdx++) {
            ConcurrentHashMap<Integer, Set<UUID>> table = tables.get(tableIdx);
            for (int hash = 0; hash < numBands; hash++) {
                int compositeKeyIndex = tableIdx * numBands + hash;
                List<UUID> uuids = updates[compositeKeyIndex];

                if (!uuids.isEmpty()) {
                    table.compute(hash, (k, bucket) -> {
                        if (bucket == null) {
                            bucket = ConcurrentHashMap.newKeySet();
                        }
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

    public CompletableFuture<Void> insertSingle(int[] metadata, UUID nodeId) {
        if (shutdownInitiated) {
            log.warn("Single insert aborted due to shutdown");
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        return CompletableFuture.runAsync(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            try {
                if (metadata == null || nodeId == null) {
                    log.warn("Null metadata or nodeId: metadata={}, nodeId={}", metadata, nodeId);
                    return;
                }
                short[] hashes = new short[numHashTables];
                for (int i = 0; i < numHashTables; i++) {
                    hashes[i] = (short) computeHash(metadata, i);
                }
                nodeHashCache.put(nodeId, hashes);

                List<ConcurrentHashMap<Integer, Set<UUID>>> currentTables = hashTables.get();
                for (int i = 0; i < numHashTables; i++) {
                    short hash = hashes[i];
                    ConcurrentHashMap<Integer, Set<UUID>> table = currentTables.get(i);
                    table.compute((int) hash, (k, bucket) -> {
                        if (bucket == null) {
                            bucket = ConcurrentHashMap.newKeySet();
                        }
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

    public Set<UUID> querySync(int[] metadata, UUID nodeId) {
        boolean shouldSample = ThreadLocalRandom.current().nextDouble() < QUERY_METRICS_SAMPLE_RATE;
        Timer.Sample sample = shouldSample ? Timer.start(meterRegistry) : null;

        List<ConcurrentHashMap<Integer, Set<UUID>>> currentTables = hashTables.get();
        Set<UUID> candidates = ConcurrentHashMap.newKeySet();
        try {
            if (metadata == null) {
                log.warn("Null metadata for query: nodeId={}", nodeId);
                return Collections.emptySet();
            }

            short[] hashes = nodeHashCache.getIfPresent(nodeId);
            if (hashes == null) {
                hashes = new short[numHashTables];
                for (int i = 0; i < numHashTables; i++) {
                    hashes[i] = (short) computeHash(metadata, i);
                }
                nodeHashCache.put(nodeId, hashes);
            }

            for (int i = 0; i < numHashTables; i++) {
                int hash = hashes[i];
                Set<UUID> bucket = currentTables.get(i).getOrDefault(hash, Collections.emptySet());
                // *** FIX: Removed strict 'topK' pruning here.
                // All candidates from the bucket are now added.
                // The 'topK' parameter from constructor can still exist for other purposes (e.g., metric bounds)
                // but should not limit candidates directly in this query logic for your use case.
                candidates.addAll(bucket);
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

    public CompletableFuture<Set<UUID>> queryAsync(int[] metadata, UUID nodeId) {
        if (shutdownInitiated) {
            log.warn("Query aborted due to shutdown");
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        try {
            return CompletableFuture.supplyAsync(() -> querySync(metadata, nodeId), executor);
        } catch (RejectedExecutionException e) {
            log.error("Query task rejected: queue full");
            throw new CompletionException("LSH query queue overloaded", e);
        }
    }

    public CompletableFuture<Map<UUID, Set<UUID>>> queryAsyncAll(List<Pair<int[], UUID>> nodeEncodings) {
        if (nodeEncodings == null || nodeEncodings.isEmpty()) {
            log.warn("Empty or null nodeEncodings list for bulk query");
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        if (shutdownInitiated) {
            log.warn("Bulk query aborted due to shutdown");
            return CompletableFuture.failedFuture(new IllegalStateException("LSHIndex is shutting down"));
        }

        return CompletableFuture.supplyAsync(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            Map<UUID, Set<UUID>> allCandidates = new ConcurrentHashMap<>();

            nodeEncodings.parallelStream().forEach(pair -> {
                int[] metadata = pair.getKey();
                UUID nodeId = pair.getValue();
                try {
                    Set<UUID> candidatesForNode = querySync(metadata, nodeId);
                    allCandidates.put(nodeId, candidatesForNode);
                } catch (Exception e) {
                    log.error("Error processing node {} in bulk query: {}", nodeId, e.getMessage());
                    meterRegistry.counter("lsh_bulk_query_node_errors").increment();
                }
            });

            sample.stop(meterRegistry.timer("lsh_bulk_query_duration"));
            meterRegistry.counter("lsh_bulk_query_total_nodes").increment(nodeEncodings.size());
            return allCandidates;
        }, executor);
    }

    private int computeHash(int[] metadata, int tableIndex) {
        if (metadata == null || metadata.length == 0) {
            throw new IllegalArgumentException("Invalid metadata array");
        }
        long seed = tableIndex * 31L;
        int hash = 0;
        for (int i = 0; i < metadata.length; i++) {
            int value = metadata[i];
            hash ^= FastHash.hash(value, seed + i);
        }
        return hash & (numBands - 1);
    }

    public long totalBucketsCount() {
        return totalEntries.get();
    }

    public boolean isBuilding() {
        return isBuilding;
    }

    private static class RingBufferSet<E> extends AbstractSet<E> {
        private final E[] buffer;
        private final int capacity;
        private int size;
        private int head;

        @SuppressWarnings("unchecked")
        public RingBufferSet(int capacity) {
            this.capacity = capacity;
            this.buffer = (E[]) new Object[capacity];
            this.size = 0;
            this.head = 0;
        }

        @Override
        public boolean add(E e) {
            if (e == null) return false;
            if (contains(e)) return false;
            if (size < capacity) {
                buffer[size++] = e;
                return true;
            }
            buffer[head] = e;
            head = (head + 1) % capacity;
            return true;
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            boolean modified = false;
            for (E e : c) {
                modified |= add(e);
            }
            return modified;
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {
                private int currentIndex = 0;
                private final Object[] snapshot;
                private final int snapshotSize;

                {
                    snapshot = new Object[size];
                    int effectiveStart = (size < capacity) ? 0 : head;
                    for (int i = 0; i < size; i++) {
                        snapshot[i] = buffer[(effectiveStart + i) % capacity];
                    }
                    snapshotSize = size;
                }

                @Override
                public boolean hasNext() {
                    return currentIndex < snapshotSize;
                }

                @SuppressWarnings("unchecked")
                @Override
                public E next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    return (E) snapshot[currentIndex++];
                }
            };
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean contains(Object o) {
            for (int i = 0; i < size; i++) {
                int index = (size < capacity) ? i : (head + i) % capacity;
                if (Objects.equals(buffer[index], o)) {
                    return true;
                }
            }
            return false;
        }

        public static <E> RingBufferSet<E> empty() {
            return new RingBufferSet<>(0);
        }
    }

    private static class FastHash {
        private static final long M = 0xc6a4a7935bd1e995L;
        private static final int R = 47;

        public static int hash(int data, long seed) {
            long h = seed ^ (data * M);
            h ^= h >>> R;
            h *= M;
            h ^= h >>> R;
            return (int) (h & 0x7FFFFFFF);
        }
    }
}