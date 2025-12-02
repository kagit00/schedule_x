package com.shedule.x.processors;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.shedule.x.config.LSHConfig;
import com.shedule.x.utils.basic.HashUtils;
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
import java.util.stream.Collectors;
import java.util.*;
import java.util.concurrent.*;


@Slf4j
public class LSHIndexImpl implements LSHIndex {
    private static final double QUERY_METRICS_SAMPLE_RATE = 0.01;
    private final int topK;
    private final MeterRegistry meterRegistry;
    private final AtomicInteger queryCounter = new AtomicInteger();
    private final DistributionSummary candidateCountSummary;
    private final Counter hashCollisionCounter;
    private final AtomicLong totalEntries = new AtomicLong();
    private volatile boolean shutdownInitiated = false;
    private final Cache<UUID, short[]> nodeHashCache;

    private final int numHashTables;
    private final int numBands;
    private final GraphStore graphStore;
    private final ExecutorService executor;
    private final ThreadLocal<Map<Integer, List<UUID>>> batchBuffer;

    public LSHIndexImpl(LSHConfig config,
                        MeterRegistry meterRegistry,
                        @Qualifier("indexExecutor") ExecutorService executor,
                        GraphStore graphStore, ThreadLocal<Map<Integer, List<UUID>>> batchBuffer) {
        this.numHashTables = config.getNumHashTables();
        this.numBands = config.getNumBands();
        this.topK = config.getTopK();
        this.meterRegistry = meterRegistry;
        this.graphStore = graphStore;
        this.executor = executor;
        this.batchBuffer = batchBuffer;
        this.candidateCountSummary = initializeCandidateCountSummary();
        this.hashCollisionCounter = initializeHashCollisionCounter();
        this.nodeHashCache = CacheBuilder.newBuilder()
                .maximumSize(10_000_000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build();
        log.info("Initialized LSHIndex with {} tables, {} bands (rows), topK={}",
                numHashTables, numBands, topK);
    }

    private DistributionSummary initializeCandidateCountSummary() {
        return DistributionSummary.builder("lsh_query_candidate_count")
                .publishPercentiles(0.5, 0.95)
                .register(meterRegistry);
    }

    @Override
    public long getNodePriorityScore(UUID nodeId) {
        short[] hashes = nodeHashCache.getIfPresent(nodeId);

        if (hashes != null && hashes.length > 0) {
            return hashes[0] & 0xFFFFL;
        }

        return nodeId.getMostSignificantBits() ^ nodeId.getLeastSignificantBits();
    }


    private Counter initializeHashCollisionCounter() {
        return Counter.builder("lsh_hash_collisions")
                .register(meterRegistry);
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
    public void clean() {
        try {
            graphStore.clearAllBuckets();
            totalEntries.set(0L);
            log.info("LSH index cleared (persistent buckets removed)");
        } catch (Exception e) {
            log.error("Failed to clean LSH index.", e);
        }
    }

    @Override
    public Set<UUID> querySync(int[] metadata, UUID nodeId) {
        boolean sample = ThreadLocalRandom.current().nextDouble() < QUERY_METRICS_SAMPLE_RATE;
        Timer.Sample timer = sample ? Timer.start(meterRegistry) : null;

        if (metadata == null || nodeId == null) {
            log.warn("Query attempted with null metadata or nodeId.");
            return Collections.emptySet();
        }

        short[] queryHashes = HashUtils.computeHashes(metadata, numHashTables, numBands);

        Map<UUID, Integer> candidateToSharedCount = new LinkedHashMap<>();

        try {
            for (int tableIdx = 0; tableIdx < numHashTables; tableIdx++) {
                int bucketHash = queryHashes[tableIdx] & 0xFFFF;
                Set<UUID> bucket = graphStore.getBucket(tableIdx, bucketHash);

                for (UUID candidate : bucket) {
                    if (candidate.equals(nodeId)) {
                        continue; // skip self
                    }
                    candidateToSharedCount.merge(candidate, 1, Integer::sum);
                }
            }


            Set<UUID> rankedCandidates = candidateToSharedCount.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .limit(topK)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toCollection(LinkedHashSet::new)); // preserves ranking order

            if (sample && timer != null) {
                timer.stop(meterRegistry.timer("lsh_query_duration_sampled"));
                candidateCountSummary.record(rankedCandidates.size());
                double avgShared = candidateToSharedCount.entrySet().stream()
                        .limit(topK)
                        .mapToInt(Map.Entry::getValue)
                        .average()
                        .orElse(0.0);
                meterRegistry.gauge("lsh_query_avg_shared_hashes", avgShared);
            }

            queryCounter.incrementAndGet();
            return rankedCandidates;

        } catch (Exception e) {
            log.error("Query failed for node {}", nodeId, e);
            return Collections.emptySet();
        } finally {
            if (sample && timer != null) {
                timer.stop(meterRegistry.timer("lsh_query_duration_sampled"));
            }
        }
    }

    @Override
    public CompletableFuture<Void> insertBatch(List<Map.Entry<int[], UUID>> entries) {
        if (entries.isEmpty()) return CompletableFuture.completedFuture(null);

        return CompletableFuture.runAsync(() -> {
            try {
                Map<Integer, List<UUID>> groupedUpdates = processBatch(entries);

                if (!groupedUpdates.isEmpty()) {
                    graphStore.bulkIngestLSH(groupedUpdates);
                }
            } catch (Exception e) {
                log.error("Batch insert failed", e);
                throw new CompletionException(e);
            }
        }, executor);
    }

    private Map<Integer, List<UUID>> processBatch(List<Map.Entry<int[], UUID>> entries) {
        Map<Integer, List<UUID>> buffer = batchBuffer.get();
        buffer.clear();

        for (var entry : entries) {
            UUID nodeId = entry.getValue();
            int[] meta = entry.getKey();

            short[] hashes = HashUtils.computeHashes(meta, numHashTables, numBands);

            for (int i = 0; i < numHashTables; i++) {
                int bucketHash = hashes[i] & 0xFFFF;
                int compositeKey = (i << 16) | bucketHash;

                buffer.computeIfAbsent(compositeKey, k -> new ArrayList<>()).add(nodeId);
            }
        }
        return buffer;
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

}