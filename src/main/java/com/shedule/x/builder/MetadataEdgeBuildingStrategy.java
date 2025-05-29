package com.shedule.x.builder;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.processors.LSHIndex;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.processors.MetadataEncoder;
import com.shedule.x.utils.db.BatchUtils;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.lang3.tuple.Pair;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class MetadataEdgeBuildingStrategy implements SymmetricEdgeBuildingStrategy {
    private final Map<UUID, Long> lastModified = new ConcurrentHashMap<>();
    private final AtomicReference<State> prepState = new AtomicReference<>(State.UNINITIALIZED);
    private final AtomicReference<CompletableFuture<Boolean>> preparationFuture =
            new AtomicReference<>(CompletableFuture.completedFuture(false));
    private final Semaphore chunkSemaphore = new Semaphore(4, true);
    private final LSHIndex lshIndex;
    private final CompatibilityCalculator compatibilityCalculator;
    private final MetadataEncoder metadataEncoder;
    private final int candidateLimit;
    private final double similarityThreshold;
    private volatile Snapshot currentSnapshot = new Snapshot(Map.of(), Map.of());
    private volatile Throwable preparationFailureCause = null;
    private final int maxRetries = 3;
    private final long retryDelayMillis = 1000;
    private final long chunkTimeoutSeconds = 30;
    private final ExecutorService executor;

    private final ThreadLocal<ArrayList<GraphRecords.PotentialMatch>> chunkMatchesBuffer =
            ThreadLocal.withInitial(() -> new ArrayList<>(32));

    enum State { UNINITIALIZED, IN_FLIGHT, SUCCESS, FAILED }

    record Snapshot(Map<UUID, Node> nodes, Map<UUID, int[]> encodedNodesCache) {
        Snapshot {
            nodes = Map.copyOf(nodes);
            encodedNodesCache = Map.copyOf(encodedNodesCache);
        }
    }

    public MetadataEdgeBuildingStrategy(
            LSHIndex lshIndex,
            CompatibilityCalculator compatibilityCalculator,
            MetadataEncoder metadataEncoder,
            Integer candidateLimit,
            Double similarityThreshold,
            ExecutorService executor) {
        this.lshIndex = lshIndex;
        this.compatibilityCalculator = compatibilityCalculator;
        this.metadataEncoder = metadataEncoder;
        this.candidateLimit = candidateLimit;
        this.similarityThreshold = similarityThreshold;
        this.executor = executor;
        log.info("Initialized MetadataEdgeBuildingStrategy (similarityThreshold={}, candidateLimit={})",
                similarityThreshold, candidateLimit);
    }

    @Override
    public void processBatch(List<Node> batch, Graph graph, Collection<GraphRecords.PotentialMatch> matches,
                             Set<Edge> edges, MatchingRequest request, Map<String, Object> context) {
        String groupId = request.getGroupId();
        log.info("Processing batch of {} nodes for groupId={}", batch.size(), groupId);

        boolean acquired = false;
        try {
            acquired = chunkSemaphore.tryAcquire(chunkTimeoutSeconds, TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timed out acquiring chunkSemaphore for groupId={}", groupId);
                return;
            }

            List<GraphRecords.PotentialMatch> chunkMatches = processBatchSync(batch, groupId, request.getDomainId());
            matches.addAll(chunkMatches);
            log.info("Completed batch for groupId={}, total matches={}, batch size={}",
                    groupId, chunkMatches.size(), batch.size());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted acquiring chunkSemaphore for groupId={}: {}", groupId, e.getMessage());
        } catch (Exception e) {
            log.error("Batch processing failed for groupId={}: {}", groupId, e.getMessage());
            throw new InternalServerErrorException("Batch processing failed for groupId=" + groupId);
        } finally {
            if (acquired) {
                chunkSemaphore.release();
                log.debug("Released chunkSemaphore for groupId={}, permits left: {}",
                        groupId, chunkSemaphore.availablePermits());
            }
        }
    }

    private List<GraphRecords.PotentialMatch> processBatchSync(List<Node> batch, String groupId, UUID domainId) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (preparationFailureCause != null || preparationFuture.get().isCompletedExceptionally()) {
                    log.error("LSH index preparation failed for groupId={}: {}",
                            groupId, preparationFailureCause != null ? preparationFailureCause.getMessage() : "Exceptional completion");
                    continue;
                }

                if (prepState.get() == State.UNINITIALIZED || prepState.get() == State.FAILED) {
                    log.info("LSH index not prepared for groupId={}, preparing (attempt {}/{})", groupId, attempt, maxRetries);
                    int finalAttempt = attempt;
                    CompletableFuture<Boolean> newPrep = indexNodes(batch, 0).thenApply(v -> true)
                            .exceptionally(e -> {
                                preparationFailureCause = e;
                                log.error("LSH preparation failed for groupId={} (attempt {}/{}): {}",
                                        groupId, finalAttempt, maxRetries, e.getMessage());
                                return false;
                            });
                    if (prepState.compareAndSet(State.UNINITIALIZED, State.IN_FLIGHT) ||
                            prepState.compareAndSet(State.FAILED, State.IN_FLIGHT)) {
                        preparationFuture.set(newPrep);
                        log.info("Triggered LSH index preparation for groupId={}", groupId);
                    }
                }

                CompletableFuture<Boolean> prepResultFuture = new CompletableFuture<>();
                preparationFuture.get().thenAcceptAsync(prepResultFuture::complete, executor).exceptionally(t -> {
                    log.error("Failed to complete preparation for groupId={}: {}", groupId, t.getMessage());
                    prepResultFuture.completeExceptionally(t);
                    return null;
                });

                Boolean prepared;
                try {
                    prepared = prepResultFuture.get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.error("Error waiting for preparation for groupId={}: {}", groupId, e.getMessage());
                    continue;
                }

                if (!prepared) {
                    log.warn("LSH preparation incomplete for groupId={} (attempt {}/{}), retrying", groupId, attempt, maxRetries);
                    prepState.set(State.FAILED);
                    if (attempt < maxRetries) {
                        try {
                            Thread.sleep(retryDelayMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    continue;
                }

                Snapshot snap = currentSnapshot;
                if (snap.encodedNodesCache().isEmpty()) {
                    log.error("encodedNodesCache empty for groupId={} after preparation, retrying", groupId);
                    if (attempt < maxRetries) {
                        try {
                            Thread.sleep(retryDelayMillis);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    continue;
                }

                List<List<Node>> nodeChunks = BatchUtils.partition(batch, 50);
                List<GraphRecords.PotentialMatch> allMatches = new ArrayList<>();

                for (List<Node> chunk : nodeChunks) {
                    boolean acquired = false;
                    try {
                        acquired = chunkSemaphore.tryAcquire(chunkTimeoutSeconds, TimeUnit.SECONDS);
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
                                .orTimeout(chunkTimeoutSeconds, TimeUnit.SECONDS);

                        CompletableFuture<Map<UUID, Set<UUID>>> queryResultFuture = new CompletableFuture<>();
                        queryFuture.thenAcceptAsync(queryResultFuture::complete, executor).exceptionally(t -> {
                            log.error("Failed to query chunk for groupId={}: {}", groupId, t.getMessage());
                            queryResultFuture.completeExceptionally(t);
                            return null;
                        });

                        Map<UUID, Set<UUID>> bulkCandidates;
                        try {
                            bulkCandidates = queryResultFuture.get(chunkTimeoutSeconds, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            log.error("Error waiting for chunk query for groupId={}: {}", groupId, e.getMessage());
                            continue;
                        }

                        List<GraphRecords.PotentialMatch> chunkMatches =
                                processChunkCandidates(chunk, bulkCandidates, groupId, domainId, snap);
                        allMatches.addAll(chunkMatches);
                        log.debug("Chunk for groupId={} produced {} matches (chunk size={})", groupId, chunkMatches.size(), chunk.size());

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Interrupted during chunk processing for groupId={}: {}", groupId, e.getMessage());
                    } catch (Exception e) {
                        log.error("Error processing chunk for groupId={}: {}", groupId, e.getMessage());
                    } finally {
                        if (acquired) {
                            chunkSemaphore.release();
                        }
                    }
                }

                log.info("Generated {} matches for groupId={}", allMatches.size(), groupId);
                return allMatches;
            } catch (Exception e) {
                log.error("Attempt {}/{} failed for groupId={}: {}", attempt, maxRetries, groupId, e.getMessage());
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(retryDelayMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry delay interrupted for groupId={}", groupId);
                    }
                }
            }
        }
        log.error("All retries failed for groupId={}", groupId);
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> indexNodes(List<Node> nodes, int page) {
        log.info("Indexing {} nodes for page={}", nodes.size(), page);
        preparationFailureCause = null;

        CompletableFuture<Void> newFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> newPrep = newFuture.thenApply(v -> {
            prepState.set(State.SUCCESS);
            return true;
        }).exceptionally(e -> {
            prepState.set(State.FAILED);
            preparationFailureCause = e;
            log.error("LSH preparation failed for page={}: {}", page, e.getMessage());
            return false;
        });

        if (!prepState.compareAndSet(State.UNINITIALIZED, State.IN_FLIGHT) &&
                !prepState.compareAndSet(State.FAILED, State.IN_FLIGHT)) {
            log.info("Joining existing indexing operation for page={}", page);
            return preparationFuture.get().thenApply(v -> null);
        }

        preparationFuture.set(newPrep);

        Map<UUID, Node> tempNodeMap = new ConcurrentHashMap<>();
        Map<UUID, int[]> tempEncodedNodesCache = new ConcurrentHashMap<>();
        AtomicInteger invalidCount = new AtomicInteger();
        AtomicInteger duplicatesCount = new AtomicInteger();
        List<Map.Entry<Map<String, String>, UUID>> rawEntries = new ArrayList<>();

        for (Node node : nodes) {
            if (node.getId() == null || node.getMetaData() == null || node.getMetaData().isEmpty()) {
                invalidCount.incrementAndGet();
                continue;
            }
            Node oldNode = tempNodeMap.putIfAbsent(node.getId(), node);
            if (oldNode != null) {
                duplicatesCount.incrementAndGet();
                continue;
            }

            long currentTimestamp = node.getMetaData().getOrDefault("_lastModified", "0").hashCode();
            Long lastTimestamp = lastModified.get(node.getId());
            if (lastTimestamp == null || lastTimestamp != currentTimestamp) {
                rawEntries.add(new AbstractMap.SimpleEntry<>(node.getMetaData(), node.getId()));
                lastModified.put(node.getId(), currentTimestamp);
            }
        }

        if (invalidCount.get() > 0) {
            log.warn("Skipped {} nodes with invalid id/metadata during indexing.", invalidCount.get());
        }
        if (duplicatesCount.get() > 0) {
            log.warn("Skipped {} duplicate node IDs during indexing.", duplicatesCount.get());
        }
        log.info("Encoding {} new or updated nodes for indexing", rawEntries.size());

        List<Map.Entry<int[], UUID>> encodedEntries = metadataEncoder.encodeBatch(rawEntries);
        if (encodedEntries.isEmpty()) {
            log.warn("No valid encoded entries for indexing, skipping LSH preparation for page={}", page);
            currentSnapshot = new Snapshot(tempNodeMap, tempEncodedNodesCache);
            prepState.set(State.SUCCESS);
            preparationFuture.set(CompletableFuture.completedFuture(true));
            newFuture.complete(null);
            return CompletableFuture.completedFuture(null);
        }

        for (Map.Entry<int[], UUID> entry : encodedEntries) {
            tempEncodedNodesCache.put(entry.getValue(), entry.getKey());
        }

        CompletableFuture<Void> indexFuture = page == 0
                ? lshIndex.prepareAsync(encodedEntries)
                : lshIndex.insertBatch(encodedEntries);

        return indexFuture.thenRun(() -> {
            currentSnapshot = new Snapshot(tempNodeMap, tempEncodedNodesCache);
            prepState.set(State.SUCCESS);
            preparationFuture.set(CompletableFuture.completedFuture(true));
            log.info("Indexing completed for page={}", page);
            newFuture.complete(null);
        }).exceptionally(e -> {
            prepState.set(State.FAILED);
            preparationFailureCause = e;
            log.error("Indexing failed for page={}: {}", page, e.getMessage());
            newFuture.completeExceptionally(e);
            throw new InternalServerErrorException("Indexing failed for page=" + page);
        });
    }

    private List<GraphRecords.PotentialMatch> processChunkCandidates(
            List<Node> chunk, Map<UUID, Set<UUID>> bulkCandidates, String groupId, UUID domainId, Snapshot snap) {
        ArrayList<GraphRecords.PotentialMatch> chunkMatches = chunkMatchesBuffer.get();
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
                                        node.getReferenceId(),
                                        candidateNode.getReferenceId(),
                                        compatibilityScore,
                                        groupId,
                                        domainId);
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
                log.error("Node processing failed for nodeId={} groupId={}: {}", node.getId(), groupId, e.getMessage());
            }
        }
        return chunkMatches;
    }
}