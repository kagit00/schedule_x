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
import java.util.function.Supplier;
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
            ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer,
            Map<String, Object> context) {

        // 1. Resolve Cancellation Supplier safely
        @SuppressWarnings("unchecked")
        Supplier<Boolean> isCancelled = (context != null && context.get("cancellationCheck") instanceof Supplier)
                ? (Supplier<Boolean>) context.get("cancellationCheck")
                : () -> false;

        final Map<UUID, NodeDTO> targetNodeMap = (targetBatch == null || targetBatch.isEmpty())
                ? Collections.emptyMap()
                : targetBatch.stream().collect(Collectors.toMap(NodeDTO::getId, Function.identity()));

        List<List<NodeDTO>> nodeChunks = BatchUtils.partition(sourceBatch, 50);
        List<GraphRecords.PotentialMatch> allMatches = new ArrayList<>();

        for (List<NodeDTO> chunk : nodeChunks) {
            // 2. High-level cancellation check
            if (Thread.currentThread().isInterrupted() || isCancelled.get()) {
                log.warn("Symmetric edge processing cancelled for groupId={}", groupId);
                break;
            }

            boolean acquired = false;
            try {
                acquired = chunkSemaphore.tryAcquire(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);
                if (!acquired) {
                    log.warn("Timed out acquiring semaphore for chunk in groupId={}", groupId);
                    continue;
                }

                // 3. Pass isCancelled to the waiting logic
                if (!waitForLshReady(currentSnapshot, chunk.size(), groupId, isCancelled)) {
                    log.warn("LSH indexing not ready or cancelled for groupId={}", groupId);
                    continue;
                }

                Snapshot snap = currentSnapshot.get();
                List<Pair<int[], UUID>> nodesForBulkQuery = chunk.stream()
                        .map(node -> {
                            int[] encoded = snap.encodedNodesCache().get(node.getId());
                            if (encoded == null) return null;
                            return Pair.of(encoded, node.getId());
                        })
                        .filter(Objects::nonNull)
                        .toList();

                if (nodesForBulkQuery.isEmpty()) continue;

                // 4. Query LSH with a clean blocking get (avoids CF zombie tasks)
                CompletableFuture<Map<UUID, Set<UUID>>> queryFuture = lshIndex.queryAsyncAll(nodesForBulkQuery);
                Map<UUID, Set<UUID>> bulkCandidates;
                try {
                    bulkCandidates = queryFuture.get(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    queryFuture.cancel(true); // Attempt to kill the LSH query task
                    log.error("LSH Query timed out for groupId={}", groupId);
                    continue;
                }

                // 5. Process candidates with per-node cancellation checks
                allMatches.addAll(processChunkCandidates(
                        chunk,
                        bulkCandidates,
                        targetNodeMap,
                        groupId,
                        domainId,
                        compatibilityCalculator,
                        config.getSimilarityThreshold(),
                        config.getCandidateLimit(),
                        chunkMatchesBuffer,
                        isCancelled
                ));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted during chunk processing for groupId={}", groupId);
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

    private boolean waitForLshReady(
            AtomicReference<Snapshot> currentSnapshot,
            int expectedSize,
            UUID groupId,
            Supplier<Boolean> isCancelled) {

        int attempts = 0;
        final int maxAttempts = 30;
        final long baseDelayMs = 3000L;

        while (attempts < maxAttempts) {
            if (Thread.currentThread().isInterrupted() || isCancelled.get()) {
                return false;
            }

            Snapshot snap = currentSnapshot.get();
            if (snap != null && snap.encodedNodesCache() != null) {
                int cached = snap.encodedNodesCache().size();
                if (cached >= expectedSize * 0.7) return true;
            }

            attempts++;
            long delay = Math.min(baseDelayMs * (1L << (attempts - 1)), 15000L);

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
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
            ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer,
            Supplier<Boolean> isCancelled) {

        List<GraphRecords.PotentialMatch> chunkMatches = chunkMatchesBuffer.get();
        chunkMatches.clear();

        for (NodeDTO node : sourceChunk) {
            if (Thread.currentThread().isInterrupted() || isCancelled.get()) {
                break;
            }

            try {
                Set<UUID> rawCandidates = bulkCandidates.getOrDefault(node.getId(), Collections.emptySet());
                if (rawCandidates.isEmpty()) continue;
                List<GraphRecords.PotentialMatch> nodeMatches = new ArrayList<>();

                for (UUID candidateId : rawCandidates) {
                    if (node.getId().equals(candidateId)) continue;

                    NodeDTO candidateNode = targetNodeMap.get(candidateId);
                    if (candidateNode == null) continue;

                    double score = compatibilityCalculator.calculate(node, candidateNode);
                    if (score >= similarityThreshold) {
                        nodeMatches.add(new GraphRecords.PotentialMatch(
                                node.getReferenceId(), candidateNode.getReferenceId(),
                                score, groupId, domainId));
                    }
                }

                nodeMatches.sort(Comparator.comparingDouble(GraphRecords.PotentialMatch::getCompatibilityScore).reversed());
                chunkMatches.addAll(nodeMatches.stream().limit(candidateLimit).toList());

            } catch (Exception e) {
                log.error("Node processing failed for nodeId={} groupId={}", node.getId(), groupId, e);
            }
        }
        return chunkMatches;
    }
}