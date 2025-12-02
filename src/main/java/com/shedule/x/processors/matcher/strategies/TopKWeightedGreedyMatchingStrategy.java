package com.shedule.x.processors.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.service.GraphRecords;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

@Slf4j
@Getter
@Component("topKWeightedGreedyMatchingStrategy")
public class TopKWeightedGreedyMatchingStrategy implements MatchingStrategy {

    private static final int BASE_SUB_BATCH_SIZE = 500;
    private static final double MEMORY_THRESHOLD_RATIO = 0.8;
    private static final int MAX_NODES_MULTIPLIER = 100;
    private static final Comparator<GraphRecords.PotentialMatch> SCORE_COMPARATOR =
            Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore)
                    .reversed()
                    .thenComparing(GraphRecords.PotentialMatch::getMatchedReferenceId);
    private Semaphore computeSemaphore;
    private static final long GC_COOLDOWN_MS = 30_000;
    private volatile long lastGCTime = 0;

    @Value("${matching.topk.count:100}")
    private int maxMatchesPerNode;
    @Value("${matching.max.memory.mb:1024}")
    private long maxMemoryMb;
    @Value("${matching.max.distinct.nodes:10000}")
    private int maxDistinctNodes;
    @Value("${matching.parallelism.level:0}")
    private int parallelismLevel;

    @PostConstruct
    public void init() {
        int effectiveParallelism = (parallelismLevel > 0) ? parallelismLevel : Runtime.getRuntime().availableProcessors();
        this.computeSemaphore = new Semaphore(Math.max(1, effectiveParallelism));
        log.info("Initialized TopKWeightedGreedyMatchingStrategy with parallelism level: {}", effectiveParallelism);
    }

    @Override
    public boolean supports(String mode) {
        return "TopKWeightedGreedyOnPotentialMatches".equalsIgnoreCase(mode);
    }

    @Override
    public Map<String, List<MatchResult>> match(List<GraphRecords.PotentialMatch> allPotentialMatches, UUID groupId, UUID domainId) {
        log.info("Processing matches: size={}, groupId={}, domainId={}", allPotentialMatches.size(), groupId, domainId);
        Objects.requireNonNull(allPotentialMatches, "Potential matches cannot be null");
        Objects.requireNonNull(groupId, "Group ID cannot be null");
        Objects.requireNonNull(domainId, "Domain ID cannot be null");

        long distinctNodes = allPotentialMatches.stream()
                .flatMap(pm -> Stream.of(pm.getReferenceId(), pm.getMatchedReferenceId()))
                .filter(Objects::nonNull)
                .distinct()
                .count();
        log.debug("Distinct nodes in input: {}", distinctNodes);
        if (distinctNodes > maxDistinctNodes) {
            throw new IllegalStateException("Too many distinct nodes (" + distinctNodes + "); max allowed " + maxDistinctNodes);
        }

        Map<String, List<MatchResult>> resultMatches = new ConcurrentHashMap<>();
        int subBatchSize = adjustBatchSize(distinctNodes);
        List<List<GraphRecords.PotentialMatch>> subBatches = partitionBatch(allPotentialMatches, subBatchSize);
        log.debug("Created {} sub-batches with size {}", subBatches.size(), subBatchSize);

        for (List<GraphRecords.PotentialMatch> subBatch : subBatches) {
            if (isMemoryThresholdExceeded()) {
                throw new IllegalStateException("Memory usage exceeds limit: " + maxMemoryMb + "MB");
            }

            Map<String, PriorityQueue<GraphRecords.PotentialMatch>> adjacencyMap = buildStreamingAdjacencyMap(subBatch, groupId, domainId);
            computeMatches(adjacencyMap, resultMatches);
            log.debug("Processed sub-batch, resultMatches size: {}", resultMatches.size());
            adjacencyMap.clear();
            subBatch.clear();
        }

        log.info("Completed matching, total matches: {}", resultMatches.size());
        return Collections.unmodifiableMap(resultMatches);
    }

    private int adjustBatchSize(long distinctNodes) {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxAllowedMemory = (long) (maxMemoryMb * 1024 * 1024 * MEMORY_THRESHOLD_RATIO);
        int adjustedSize = BASE_SUB_BATCH_SIZE;

        if (usedMemory < maxAllowedMemory * 0.3 && distinctNodes < maxDistinctNodes * 0.3) {
            adjustedSize = Math.min(BASE_SUB_BATCH_SIZE * 4, 2000);
        } else if (usedMemory < maxAllowedMemory * 0.5 && distinctNodes < maxDistinctNodes * 0.5) {
            adjustedSize = Math.min(BASE_SUB_BATCH_SIZE * 2, 1000);
        } else if (usedMemory > maxAllowedMemory * 0.5 || distinctNodes > maxDistinctNodes * 0.5) {
            adjustedSize = Math.max(100, BASE_SUB_BATCH_SIZE / 2);
        } else if (usedMemory > maxAllowedMemory * 0.75 || distinctNodes > maxDistinctNodes * 0.75) {
            adjustedSize = Math.max(50, BASE_SUB_BATCH_SIZE / 4);
        }
        log.debug("Adjusted sub-batch size: {}, usedMemory: {} MB, distinctNodes: {}", adjustedSize, usedMemory / (1024 * 1024), distinctNodes);
        return adjustedSize;
    }

    private List<List<GraphRecords.PotentialMatch>> partitionBatch(List<GraphRecords.PotentialMatch> batch, int subBatchSize) {
        List<List<GraphRecords.PotentialMatch>> subBatches = new ArrayList<>();
        for (int i = 0; i < batch.size(); i += subBatchSize) {
            subBatches.add(new ArrayList<>(batch.subList(i, Math.min(i + subBatchSize, batch.size()))));
        }
        return subBatches;
    }

    private boolean isMemoryThresholdExceeded() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        if (usedMemory > maxMemoryMb * 0.75) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastGCTime > GC_COOLDOWN_MS) {
                log.debug("Triggering GC due to high memory usage: {} MB", usedMemory);
                System.gc();
                lastGCTime = currentTime;
                usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            }
        }
        return usedMemory > maxMemoryMb;
    }

    private void offerToAdjacencyMap(Map<String, PriorityQueue<GraphRecords.PotentialMatch>> map,
                                     String key, GraphRecords.PotentialMatch match) {
        map.compute(key, (k, queue) -> {
            if (queue == null) {
                queue = new PriorityQueue<>(maxMatchesPerNode + 1, SCORE_COMPARATOR);
            }
            if (queue.size() > maxMatchesPerNode * 10) {
                log.warn("Skipping node with excessive matches: {}", key);
                return queue;
            }
            queue.offer(match);
            if (queue.size() > maxMatchesPerNode) {
                queue.poll();
            }
            return queue;
        });
    }

    private Map<String, PriorityQueue<GraphRecords.PotentialMatch>> buildStreamingAdjacencyMap(
            List<GraphRecords.PotentialMatch> matches, UUID groupId, UUID domainId) {
        if (matches.size() > maxMatchesPerNode * MAX_NODES_MULTIPLIER) {
            throw new IllegalStateException("Too many nodes (" + matches.size() + "); max allowed " + (maxMatchesPerNode * MAX_NODES_MULTIPLIER));
        }

        Map<String, PriorityQueue<GraphRecords.PotentialMatch>> adjacencyMap = new ConcurrentHashMap<>();

        long distinctNodes = matches.stream()
                .flatMap(pm -> Stream.of(pm.getReferenceId(), pm.getMatchedReferenceId()))
                .filter(Objects::nonNull)
                .distinct()
                .count();
        if (distinctNodes > maxDistinctNodes) {
            throw new IllegalStateException("Sub-batch contains too many distinct nodes (" + distinctNodes + "); max allowed " + maxDistinctNodes);
        }

        int skippedMatches = 0;
        for (GraphRecords.PotentialMatch pm : matches) {
            if (!groupId.equals(pm.getGroupId()) || !domainId.equals(pm.getDomainId())) {
                log.debug("Skipping match due to groupId/domainId mismatch: {}", pm);
                skippedMatches++;
                continue;
            }

            if (pm.getReferenceId() == null || pm.getMatchedReferenceId() == null) {
                log.warn("Skipping invalid match: referenceId or matchedReferenceId is null: {}", pm);
                skippedMatches++;
                continue;
            }

            offerToAdjacencyMap(adjacencyMap, pm.getReferenceId(), pm);
            offerToAdjacencyMap(adjacencyMap, pm.getMatchedReferenceId(),
                    new GraphRecords.PotentialMatch(
                            pm.getMatchedReferenceId(),
                            pm.getReferenceId(),
                            pm.getCompatibilityScore(),
                            pm.getGroupId(),
                            pm.getDomainId()));
        }
        if (skippedMatches > 0) {
            log.info("Skipped {} matches in sub-batch due to validation or mismatch", skippedMatches);
        }

        long distinctGroupIds = adjacencyMap.values().stream()
                .flatMap(queue -> queue.stream().map(GraphRecords.PotentialMatch::getGroupId))
                .distinct()
                .count();
        long distinctDomainIds = adjacencyMap.values().stream()
                .flatMap(queue -> queue.stream().map(GraphRecords.PotentialMatch::getDomainId))
                .distinct()
                .count();
        if (distinctGroupIds > 1 || distinctDomainIds > 1) {
            throw new IllegalArgumentException("Mixed groupId or domainId in potential matches");
        }

        log.debug("Built adjacency map with {} nodes", adjacencyMap.size());
        return adjacencyMap;
    }

    private void computeMatches(Map<String, PriorityQueue<GraphRecords.PotentialMatch>> adjacencyMap,
                                Map<String, List<MatchResult>> resultMatches) {
        Iterator<String> nodeIterator = adjacencyMap.keySet().iterator();
        List<String> nodeBatch = new ArrayList<>(BASE_SUB_BATCH_SIZE);

        while (nodeIterator.hasNext()) {
            nodeBatch.clear();
            for (int i = 0; i < BASE_SUB_BATCH_SIZE && nodeIterator.hasNext(); i++) {
                nodeBatch.add(nodeIterator.next());
            }
            Collections.shuffle(nodeBatch);

            Stream<String> nodeStream = nodeBatch.size() > Runtime.getRuntime().availableProcessors() * 1000
                    ? nodeBatch.parallelStream()
                    : nodeBatch.stream();

            nodeStream.forEach(nodeId -> {
                try {
                    computeSemaphore.acquire();
                    processNode(nodeId, adjacencyMap, resultMatches);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    computeSemaphore.release();
                }
            });
        }
    }

    private void processNode(String nodeId, Map<String, PriorityQueue<GraphRecords.PotentialMatch>> adjacencyMap,
                             Map<String, List<MatchResult>> resultMatches) {
        PriorityQueue<GraphRecords.PotentialMatch> outgoers = adjacencyMap.getOrDefault(nodeId, new PriorityQueue<>());
        if (outgoers.isEmpty()) {
            return;
        }

        List<MatchResult> topMatches = new ArrayList<>();
        for (GraphRecords.PotentialMatch pm : outgoers) {
            if (!nodeId.equals(pm.getMatchedReferenceId())) {
                topMatches.add(MatchResult.builder()
                        .partnerId(pm.getMatchedReferenceId())
                        .score(pm.getCompatibilityScore())
                        .build());
            }
        }

        if (!topMatches.isEmpty()) {
            resultMatches.compute(nodeId, (k, existing) -> {
                if (existing != null) {
                    log.debug("Node {} already processed, skipping update", nodeId);
                    return existing;
                }
                return List.copyOf(topMatches);
            });
        }
        adjacencyMap.remove(nodeId);
    }
}