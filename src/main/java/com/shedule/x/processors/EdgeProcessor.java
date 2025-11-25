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
import java.util.stream.Collectors;

@Slf4j
@Component
public final class EdgeProcessor {

    private final NodeDataService nodeDataService;

    public EdgeProcessor(NodeDataService nodeDataService) {
        this.nodeDataService = nodeDataService;
    }

    public List<GraphRecords.PotentialMatch> processBatchSync(
            List<NodeDTO> sourceBatch,
            List<NodeDTO> targetBatch,
            UUID groupId,
            UUID domainId,
            LSHIndex lshIndex,
            CompatibilityCalculator compatibilityCalculator,
            MetadataEncoder metadataEncoder,
            AtomicReference<Snapshot> currentSnapshot,
            EdgeBuildingConfig config,
            Semaphore chunkSemaphore,
            ExecutorService executor,
            ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer) {

        final Set<UUID> targetNodeIds = (targetBatch == null || targetBatch.isEmpty())
                ? Collections.emptySet()
                : targetBatch.stream().map(NodeDTO::getId).collect(Collectors.toSet());

        for (int attempt = 1; attempt <= config.getMaxRetries(); attempt++) {
            try {
                if (!IndexUtils.ensurePrepared(groupId, attempt, config.getMaxRetries())) {
                    log.warn("LSH preparation incomplete for groupId={} (attempt {}/{}), retrying", groupId, attempt, config.getMaxRetries());
                    Thread.sleep(config.getRetryDelayMillis());
                    continue;
                }

                Snapshot snap = currentSnapshot.get();
                if (snap.encodedNodesCache().isEmpty()) {
                    log.error("encodedNodesCache empty for groupId={} after preparation, retrying", groupId);
                    Thread.sleep(config.getRetryDelayMillis());
                    continue;
                }

                // 2. Partition the SOURCE batch for semaphore control
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

                        List<Pair<int[], UUID>> nodesForBulkQuery = chunk.stream()
                                .map(node -> { // 'node' is now NodeDTO
                                    int[] encoded = snap.encodedNodesCache().get(node.getId());
                                    if (encoded == null) {
                                        log.warn("Encoded metadata not found for node: {} in groupId={}", node.getId(), groupId);
                                        return null;
                                    }
                                    return Pair.of(encoded, node.getId());
                                })
                                .filter(Objects::nonNull)
                                .toList();

                        if (nodesForBulkQuery.isEmpty()) {
                            log.warn("No valid nodes for chunk processing in groupId={}", groupId);
                            continue;
                        }

                        // 3. Query LSH (Returns global candidates)
                        CompletableFuture<Map<UUID, Set<UUID>>> queryFuture = lshIndex
                                .queryAsyncAll(nodesForBulkQuery)
                                .orTimeout(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);

                        Map<UUID, Set<UUID>> bulkCandidates = queryFuture.get(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);

                        // 4. Process Matches with Target Filtering
                        allMatches.addAll(processChunkCandidates(
                                chunk,
                                bulkCandidates,
                                targetNodeIds,
                                groupId,
                                domainId,
                                compatibilityCalculator,
                                config.getSimilarityThreshold(),
                                config.getCandidateLimit(),
                                chunkMatchesBuffer
                        ));

                        log.debug("Chunk for groupId={} produced {} matches (chunk size={})", groupId, allMatches.size(), chunk.size());

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Interrupted during chunk processing for groupId={}", groupId, e);
                    } catch (Exception e) {
                        log.error("Error processing chunk for groupId={}", groupId, e);
                    } finally {
                        if (acquired) {
                            chunkSemaphore.release();
                        }
                    }
                }
                log.info("Generated {} matches for groupId={}", allMatches.size(), groupId);
                return allMatches;

            } catch (Exception e) {
                log.error("Attempt {}/{} failed for groupId={}", attempt, config.getMaxRetries(), groupId, e);
                if (attempt < config.getMaxRetries()) {
                    try {
                        Thread.sleep(config.getRetryDelayMillis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        log.error("All retries failed for groupId={}", groupId);
        return Collections.emptyList();
    }

    public List<GraphRecords.PotentialMatch> processChunkCandidates(
            // ⚠️ CHANGE: Input sourceChunk must be NodeDTO
            List<NodeDTO> sourceChunk,
            Map<UUID, Set<UUID>> bulkCandidates,
            Set<UUID> targetNodeIds,
            UUID groupId,
            UUID domainId,
            CompatibilityCalculator compatibilityCalculator,
            double similarityThreshold,
            int candidateLimit,
            ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer) {

        List<GraphRecords.PotentialMatch> chunkMatches = chunkMatchesBuffer.get();
        chunkMatches.clear();

        for (NodeDTO node : sourceChunk) { // 'node' is now NodeDTO
            try {
                Set<UUID> rawCandidates = bulkCandidates.getOrDefault(node.getId(), Collections.emptySet());
                if (rawCandidates.isEmpty()) {
                    continue;
                }

                List<GraphRecords.PotentialMatch> nodeMatches = rawCandidates.stream()
                        .filter(candidateId -> !node.getId().equals(candidateId))
                        .filter(targetNodeIds::contains)
                        .map(candidateId -> {
                            NodeDTO candidateNode = nodeDataService.getNode(candidateId, groupId);

                            if (candidateNode == null) {
                                log.warn("Candidate node {} not found in disk store for groupId={}", candidateId, groupId);
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