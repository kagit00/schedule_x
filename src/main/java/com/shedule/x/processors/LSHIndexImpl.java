package com.shedule.x.processors;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.shedule.x.config.LSHConfig;
import com.shedule.x.utils.basic.HashUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;

import java.util.*;
import io.micrometer.core.instrument.MeterRegistry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import org.apache.commons.lang3.tuple.Pair;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Qualifier;
import java.util.*;
import java.util.concurrent.*;


@Slf4j
public class LSHIndexImpl implements LSHIndex {

    private static final double QUERY_METRICS_SAMPLE_RATE = 0.009;

    private final int numBands;
    private final int numHashTables;

    // Dependencies
    private final MeterRegistry meterRegistry;
    private final GraphStore graphStore;
    @Qualifier("indexExecutor") private final ExecutorService executor;

    // Metrics
    private final Timer queryTimer;
    private final DistributionSummary candidateSummary;
    private final Counter insertSuccessCounter;
    private final Counter insertFailureCounter;

    // Cache: node → band hashes
    private final Cache<UUID, short[]> nodeHashCache = CacheBuilder.newBuilder()
            .maximumSize(20_000_000)
            .expireAfterWrite(120, TimeUnit.MINUTES)
            .concurrencyLevel(64)
            .recordStats()
            .build();

    // Thread-local ultra-fast buffers
    private final ThreadLocal<Int2IntOpenHashMap> scoreMap =
            ThreadLocal.withInitial(() -> new Int2IntOpenHashMap(4096));

    private final ThreadLocal<Int2ObjectOpenHashMap<UUID>> idMap =
            ThreadLocal.withInitial(() -> new Int2ObjectOpenHashMap<>(4096));

    private final ThreadLocal<Long2ObjectOpenHashMap<ArrayList<UUID>>> batchBuffer =
            ThreadLocal.withInitial(() -> {
                Long2ObjectOpenHashMap<ArrayList<UUID>> map = new Long2ObjectOpenHashMap<>(2048);
                map.defaultReturnValue(null);
                return map;
            });

    private record Candidate(int hash, int score) implements Comparable<Candidate> {
        @Override public int compareTo(Candidate o) {
            return Integer.compare(o.score, this.score);
        }
    }

    public LSHIndexImpl(LSHConfig config,
                        MeterRegistry meterRegistry,
                        @Qualifier("indexExecutor") ExecutorService executor,
                        GraphStore graphStore) {

        this.numHashTables = config.getNumHashTables();
        this.numBands = config.getNumBands();
        this.meterRegistry = meterRegistry;
        this.graphStore = graphStore;
        this.executor = executor;

        if (numHashTables <= 0 || numBands <= 0) {
            throw new IllegalArgumentException("numHashTables and numBands must be > 0");
        }
        if (numHashTables % numBands != 0) {
            throw new IllegalArgumentException("numHashTables must be divisible by numBands");
        }

        // Initialize metrics
        this.queryTimer = meterRegistry.timer("lsh.query.duration");
        this.candidateSummary = DistributionSummary.builder("lsh.query.candidate_count")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.insertSuccessCounter = meterRegistry.counter("lsh.insert_batch.success");
        this.insertFailureCounter = meterRegistry.counter("lsh.insert_batch.failure");

        log.info("LSHIndex initialized (NO LIMITS): {} bands × {} rows = {} hashes, returning ALL candidates",
                numBands, numHashTables / numBands, numHashTables);
    }

    @Override
    public Set<UUID> querySync(int[] metadata, UUID nodeId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        boolean doSample = ThreadLocalRandom.current().nextDouble() < QUERY_METRICS_SAMPLE_RATE;

        try {
            short[] bandHashes = HashUtils.computeHashes(metadata, numHashTables, numBands);

            Int2IntOpenHashMap scores = scoreMap.get();
            Int2ObjectOpenHashMap<UUID> ids = idMap.get();
            scores.clear();
            ids.clear();

            for (int band = 0; band < numBands; band++) {
                if (band % 5 == 0 && Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("LSH Band Query Interrupted");
                }

                int bucketHash = bandHashes[band] & 0xFFFF;
                Set<UUID> bucket = graphStore.getBucket(band, bucketHash);
                if (bucket == null || bucket.isEmpty()) continue;

                for (UUID candidate : bucket) {
                    if (candidate.equals(nodeId)) continue;
                    int hash = candidate.hashCode();
                    if (ids.putIfAbsent(hash, candidate) == null) {
                        scores.put(hash, 1);
                    } else {
                        scores.addTo(hash, 1);
                    }
                }
            }

            PriorityQueue<Candidate> pq = new PriorityQueue<>();

            scores.int2IntEntrySet().fastForEach(e -> {
                pq.offer(new Candidate(e.getIntKey(), e.getIntValue()));
            });

            LinkedHashSet<UUID> result = new LinkedHashSet<>(pq.size());
            while (!pq.isEmpty()) {
                result.add(ids.get(pq.poll().hash()));
            }

            if (doSample) {
                sample.stop(queryTimer);
                candidateSummary.record(result.size());
            }

            return result;

        } catch (Exception e) {
            log.error("LSH query failed for nodeId={}", nodeId, e);
            meterRegistry.counter("lsh.query.failure").increment();
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private Long2ObjectMap<List<UUID>> processBatch(List<Map.Entry<int[], UUID>> entries) {
        Long2ObjectOpenHashMap<ArrayList<UUID>> buffer = batchBuffer.get();
        buffer.clear();

        for (Map.Entry<int[], UUID> e : entries) {
            UUID nodeId = e.getValue();
            int[] features = e.getKey();

            short[] bandHashes = HashUtils.computeHashes(features, numHashTables, numBands);
            nodeHashCache.put(nodeId, bandHashes);

            for (int band = 0; band < numBands; band++) {
                long key = ((long) band << 32) | (bandHashes[band] & 0xFFFFFFFFL);
                buffer.computeIfAbsent(key, k -> new ArrayList<>(64)).add(nodeId);
            }
        }

        return (Long2ObjectMap<List<UUID>>) (Object) buffer;
    }

    @Override
    public CompletableFuture<Void> insertBatch(List<Map.Entry<int[], UUID>> entries) {
        if (entries.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final int entryCount = entries.size();
        meterRegistry.counter("lsh.insert_batch.requests").increment();

        long startTime = System.nanoTime();

        // Step 1: Process batch on executor (CPU-bound: compute hashes and group)
        return CompletableFuture.supplyAsync(() -> {
                    long processStart = System.nanoTime();

                    try {
                        Long2ObjectMap<List<UUID>> grouped = processBatch(entries);

                        long processDuration = System.nanoTime() - processStart;
                        meterRegistry.timer("lsh.insert_batch.process_time")
                                .record(processDuration, TimeUnit.NANOSECONDS);
                        meterRegistry.summary("lsh.insert_batch.buckets_created")
                                .record(grouped.size());

                        log.debug("Processed LSH batch: {} entries into {} buckets ({}ms)",
                                entryCount, grouped.size(), TimeUnit.NANOSECONDS.toMillis(processDuration));

                        return grouped;

                    } catch (Exception e) {
                        log.error("Failed to process LSH batch of {} entries", entryCount, e);
                        meterRegistry.counter("lsh.insert_batch.process_failure").increment();
                        throw new CompletionException("LSH batch processing failed", e);
                    }
                }, executor)

                // Step 2: Chain to bulk ingest (enqueues to unified writer queue)
                .thenCompose(grouped -> {
                    if (grouped.isEmpty()) {
                        log.debug("No buckets to write for batch of {} entries", entryCount);
                        return CompletableFuture.completedFuture(null);
                    }

                    // This returns when ALL chunks are written by the unified writer thread
                    return graphStore.bulkIngestLSH(grouped);
                })

                // Step 3: Record final metrics and handle completion
                .whenComplete((result, error) -> {
                    long totalDuration = System.nanoTime() - startTime;
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(totalDuration);

                    if (error != null) {
                        insertFailureCounter.increment();
                        meterRegistry.timer("lsh.insert_batch.total_duration", "status", "failed")
                                .record(totalDuration, TimeUnit.NANOSECONDS);

                        log.error("LSH batch insertion failed: {} entries after {}ms",
                                entryCount, durationMs, error);
                    } else {
                        insertSuccessCounter.increment();
                        meterRegistry.timer("lsh.insert_batch.total_duration", "status", "success")
                                .record(totalDuration, TimeUnit.NANOSECONDS);

                        log.debug("LSH batch insertion completed: {} entries in {}ms",
                                entryCount, durationMs);
                    }
                });
    }

    @Override
    public CompletableFuture<Set<UUID>> queryAsync(int[] metadata, UUID nodeId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return querySync(metadata, nodeId);
            } catch (Exception e) {
                log.error("Async LSH query failed for nodeId={}", nodeId, e);
                throw new CompletionException("LSH query failed", e);
            }
        }, executor);
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
    public void clean() {
        try {
            log.info("Cleaning LSH index...");

            long startTime = System.nanoTime();
            graphStore.clearAllBuckets();
            nodeHashCache.invalidateAll();

            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

            meterRegistry.counter("lsh.clean.success").increment();
            log.info("LSH index cleared — all buckets removed, cache cleared ({}ms)", durationMs);

        } catch (Exception e) {
            meterRegistry.counter("lsh.clean.failure").increment();
            log.error("Failed to clean LSH index", e);
            throw new RuntimeException("LSH index clean failed", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down LSHIndex...");

        try {
            executor.shutdown();

            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate gracefully, forcing shutdown");
                executor.shutdownNow();

                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error("Executor did not terminate after forceful shutdown");
                }
            }

            nodeHashCache.invalidateAll();

            log.info("LSHIndex shutdown complete");

        } catch (InterruptedException e) {
            log.error("LSHIndex shutdown interrupted", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public CompletableFuture<Map<UUID, Set<UUID>>> queryAsyncAll(List<Pair<int[], UUID>> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        final int nodeCount = nodes.size();

        return CompletableFuture.supplyAsync(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);

            try {
                Map<UUID, Set<UUID>> result = new ConcurrentHashMap<>(nodeCount);

                for (Pair<int[], UUID> p : nodes) {

                    if (Thread.currentThread().isInterrupted()) {
                        log.warn("Bulk LSH query interrupted/cancelled. Stopping mid-batch.");
                        break;
                    }

                    try {
                        result.put(p.getValue(), querySync(p.getKey(), p.getValue()));
                    } catch (Exception e) {
                        log.error("Failed to query node {} in bulk query", p.getValue(), e);
                        meterRegistry.counter("lsh.bulk_query.node_failure").increment();
                    }
                }

                sample.stop(meterRegistry.timer("lsh.bulk_query.duration"));
                return result;

            } catch (Exception e) {
                meterRegistry.counter("lsh.bulk_query.failure").increment();
                throw new CompletionException("Bulk LSH query failed", e);
            }
        }, executor);
    }
}