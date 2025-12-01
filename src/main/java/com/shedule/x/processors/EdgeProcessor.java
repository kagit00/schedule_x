package com.shedule.x.processors;

import com.shedule.x.config.EdgeBuildingConfig;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.dto.Snapshot;
import com.shedule.x.models.Node;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.service.NodeDataService;
import com.shedule.x.utils.basic.IndexUtils;
import com.shedule.x.utils.db.BatchUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public final class EdgeProcessor {

    public List<GraphRecords.PotentialMatch> processBatchSync(
            List<NodeDTO> sourceBatch,
            List<NodeDTO> targetBatch,
            UUID groupId,
            UUID domainId,
            LSHIndex lshIndex,
            CompatibilityCalculator compatibilityCalculator,
            AtomicReference<Snapshot> currentSnapshot,
            EdgeBuildingConfig config,
            Semaphore chunkSemaphore,
            ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer) {

        final Map<UUID, NodeDTO> targetNodeMap = (targetBatch == null || targetBatch.isEmpty())
                ? Collections.emptyMap()
                : targetBatch.stream().collect(Collectors.toMap(NodeDTO::getId, Function.identity()));

        List<List<NodeDTO>> nodeChunks = BatchUtils.partition(sourceBatch, 50);
        List<GraphRecords.PotentialMatch> allMatches = new ArrayList<>();

        for (List<NodeDTO> chunk : nodeChunks) {
            boolean acquired = false;
            try {
                acquired = chunkSemaphore.tryAcquire(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);
                if (!acquired) {
                    log.warn("Timed out acquiring semaphore for chunk in groupId={}", groupId);
                    continue;
                }

                if (!waitForLshReady(currentSnapshot, chunk.size(), groupId)) {
                    log.warn("LSH indexing not ready in time for chunk in groupId={} — skipping this chunk", groupId);
                    continue;
                }

                Snapshot snap = currentSnapshot.get();
                List<Pair<int[], UUID>> nodesForBulkQuery = chunk.stream()
                        .map(node -> {
                            int[] encoded = snap.encodedNodesCache().get(node.getId());
                            if (encoded == null) {
                                log.warn("Encoded vector missing for node {} in groupId={} (should not happen)", node.getId(), groupId);
                                return null;
                            }
                            return Pair.of(encoded, node.getId());
                        })
                        .filter(Objects::nonNull)
                        .toList();

                if (nodesForBulkQuery.isEmpty()) {
                    continue;
                }

                CompletableFuture<Map<UUID, Set<UUID>>> queryFuture = lshIndex
                        .queryAsyncAll(nodesForBulkQuery)
                        .orTimeout(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);

                Map<UUID, Set<UUID>> bulkCandidates = queryFuture.get(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);

                allMatches.addAll(processChunkCandidates(
                        chunk,
                        bulkCandidates,
                        targetNodeMap,
                        groupId,
                        domainId,
                        compatibilityCalculator,
                        config.getSimilarityThreshold(),
                        config.getCandidateLimit(),
                        chunkMatchesBuffer
                ));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted during chunk processing for groupId={}", groupId, e);
                break;
            } catch (Exception e) {
                log.error("Error processing chunk for groupId={}", groupId, e);
            } finally {
                if (acquired) {
                    chunkSemaphore.release();
                }
            }
        }

        log.info("Generated {} potential matches for groupId={}", allMatches.size(), groupId);
        return allMatches;
    }

    private boolean waitForLshReady(AtomicReference<Snapshot> currentSnapshot, int expectedSize, UUID groupId) {
        int attempts = 0;
        final int maxAttempts = 30;
        final long baseDelayMs = 3000L;

        while (attempts < maxAttempts) {
            Snapshot snap = currentSnapshot.get();
            if (snap != null && snap.encodedNodesCache() != null) {
                int cached = snap.encodedNodesCache().size();
                if (cached >= expectedSize * 0.7) {
                    if (attempts > 0) {
                        log.info("LSH ready for groupId={} after {} attempts (cache={})", groupId, attempts, cached);
                    }
                    return true;
                }
            }

            attempts++;
            long delay = Math.min(baseDelayMs * (1L << (attempts - 1)), 15000L); // exponential → max 15s
            if (attempts <= 3 || attempts % 5 == 0) {
                log.info("Waiting for LSH indexing — groupId={}, attempt {}/{}, cache has {} nodes",
                        groupId, attempts, maxAttempts, snap == null ? 0 : snap.encodedNodesCache().size());
            }

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        log.error("LSH indexing took too long for groupId={} — giving up after {} attempts", groupId, maxAttempts);
        return false;
    }

    public List<GraphRecords.PotentialMatch> processChunkCandidates(
            List<NodeDTO> sourceChunk,
            Map<UUID, Set<UUID>> bulkCandidates,
            Map<UUID, NodeDTO> targetNodeMap,
            UUID groupId,
            UUID domainId,
            CompatibilityCalculator compatibilityCalculator,
            double similarityThreshold,
            int candidateLimit,
            ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer) {

        List<GraphRecords.PotentialMatch> chunkMatches = chunkMatchesBuffer.get();
        chunkMatches.clear();

        for (NodeDTO node : sourceChunk) {
            try {
                Set<UUID> rawCandidates = bulkCandidates.getOrDefault(node.getId(), Collections.emptySet());
                if (rawCandidates.isEmpty()) {
                    continue;
                }

                List<GraphRecords.PotentialMatch> nodeMatches = rawCandidates.stream()
                        .filter(candidateId -> !node.getId().equals(candidateId))

                        .filter(targetNodeMap::containsKey)

                        .map(candidateId -> {
                            NodeDTO candidateNode = targetNodeMap.get(candidateId);

                            if (candidateNode == null) {
                                return null;
                            }

                            double compatibilityScore = compatibilityCalculator.calculate(node, candidateNode);
                            if (compatibilityScore >= similarityThreshold) {
                                return new GraphRecords.PotentialMatch(
                                        node.getReferenceId(), candidateNode.getReferenceId(),
                                        compatibilityScore, groupId, domainId);
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .sorted(Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore).reversed())
                        .limit(candidateLimit)
                        .toList();

                chunkMatches.addAll(nodeMatches);
            } catch (Exception e) {
                log.error("Node processing failed for nodeId={} groupId={}", node.getId(), groupId, e);
            }
        }
        return chunkMatches;
    }
}