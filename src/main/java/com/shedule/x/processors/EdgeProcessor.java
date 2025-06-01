package com.shedule.x.processors;

import com.shedule.x.config.EdgeBuildingConfig;
import com.shedule.x.dto.Snapshot;
import com.shedule.x.models.Node;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.service.GraphRecords;
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

@Slf4j
@Component
public final class EdgeProcessor {
    public List<GraphRecords.PotentialMatch> processBatchSync(List<Node> batch, String groupId, UUID domainId,
                                                              LSHIndex lshIndex, CompatibilityCalculator compatibilityCalculator,
                                                              MetadataEncoder metadataEncoder, AtomicReference<Snapshot> currentSnapshot,
                                                              EdgeBuildingConfig config, Semaphore chunkSemaphore,
                                                              ExecutorService executor, ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer) {
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

                List<List<Node>> nodeChunks = BatchUtils.partition(batch, 50);
                List<GraphRecords.PotentialMatch> allMatches = new ArrayList<>();

                for (List<Node> chunk : nodeChunks) {
                    boolean acquired = false;
                    try {
                        acquired = chunkSemaphore.tryAcquire(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);
                        if (!acquired) {
                            log.warn("Timed out acquiring semaphore for chunk in groupId={}", groupId);
                            continue;
                        }

                        List<Pair<int[], UUID>> nodesForBulkQuery = chunk.stream()
                                .map(node -> {
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

                        CompletableFuture<Map<UUID, Set<UUID>>> queryFuture = lshIndex
                                .queryAsyncAll(nodesForBulkQuery)
                                .orTimeout(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);

                        Map<UUID, Set<UUID>> bulkCandidates = queryFuture.get(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);
                        allMatches.addAll(processChunkCandidates(chunk, bulkCandidates, groupId, domainId, snap,
                                compatibilityCalculator, config.getSimilarityThreshold(), config.getCandidateLimit(), chunkMatchesBuffer));
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

    public List<GraphRecords.PotentialMatch> processChunkCandidates(List<Node> chunk, Map<UUID, Set<UUID>> bulkCandidates,
                                                                            String groupId, UUID domainId, Snapshot snap,
                                                                            CompatibilityCalculator compatibilityCalculator,
                                                                            double similarityThreshold, int candidateLimit,
                                                                            ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer) {
        List<GraphRecords.PotentialMatch> chunkMatches = chunkMatchesBuffer.get();
        chunkMatches.clear();

        for (Node node : chunk) {
            try {
                Set<UUID> candidateIds = bulkCandidates.getOrDefault(node.getId(), Collections.emptySet());
                if (candidateIds.isEmpty()) {
                    log.debug("No candidates for nodeId={} in groupId={}", node.getId(), groupId);
                    continue;
                }

                List<GraphRecords.PotentialMatch> nodeMatches = candidateIds.stream()
                        .filter(candidateId -> !node.getId().equals(candidateId))
                        .map(candidateId -> {
                            Node candidateNode = snap.nodes().get(candidateId);
                            if (candidateNode == null) {
                                log.debug("Candidate node {} not found for nodeId={}", candidateId, node.getId());
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
                log.debug("Generated {} matches for nodeId={} in groupId={}", nodeMatches.size(), node.getId(), groupId);
            } catch (Exception e) {
                log.error("Node processing failed for nodeId={} groupId={}", node.getId(), groupId, e);
            }
        }
        return chunkMatches;
    }
}