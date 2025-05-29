package com.shedule.x.builder;


import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Graph;
import com.shedule.x.models.Node;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.processors.PotentialMatchSaver;
import com.shedule.x.service.GraphRecords;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class FlatEdgeBuildingStrategy implements SymmetricEdgeBuildingStrategy {
    private static final int STORAGE_BATCH_SIZE = 1000;
    private static final int LOG_THROTTLE_INTERVAL = 1000;

    private final PotentialMatchSaver saver;
    private final Map<UUID, Node> nodeById;
    private final Executor executor;

    public FlatEdgeBuildingStrategy(
            List<Node> nodes,
            PotentialMatchSaver saver,
            Executor executor
    ) {
        this.saver = saver;
        this.nodeById = nodes.stream().collect(Collectors.toConcurrentMap(Node::getId, n -> n));
        this.executor = executor;
    }

    @AllArgsConstructor
    @Getter
    public static class ProcessContext {
        private final Map<UUID, Node> nodeById;
        private final Set<Edge> edges;
        private final Collection<GraphRecords.PotentialMatch> matches;
        private final List<PotentialMatchEntity> batch;
        private final AtomicInteger counter;
        private final String groupId;
        private final UUID domainId;
    }

    @Override
    public void processBatch(List<Node> batch, Graph graph, Collection<GraphRecords.PotentialMatch> matches, Set<Edge> edges, MatchingRequest request, Map<String, Object> context) {
        String groupId = request.getGroupId();
        log.info("Processing batch of {} nodes for groupId={}", batch.size(), groupId);
        try {
            AtomicInteger counter = new AtomicInteger();
            List<PotentialMatchEntity> sharedBatch = Collections.synchronizedList(new ArrayList<>());
            ProcessContext ctx = new ProcessContext(nodeById, edges, matches, sharedBatch, counter, groupId, request.getDomainId());
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(batch.stream()
                    .map(node -> CompletableFuture.runAsync(() -> processNode(node, ctx), executor))
                    .toArray(CompletableFuture[]::new));

            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            allFutures.thenAcceptAsync(v -> {
                try {
                    if (!sharedBatch.isEmpty()) {
                        saver.saveMatchesAsync(sharedBatch, groupId, request.getDomainId(), "", false);
                    }
                    log.info("Completed batch for groupId={}, generated {} matches", groupId, matches.size());
                    resultFuture.complete(null);
                } catch (Exception e) {
                    log.error("Batch processing failed for groupId={}: {}", groupId, e.getMessage(), e);
                    resultFuture.completeExceptionally(new InternalServerErrorException("Batch edge building failed"));
                }
            }, executor);

            resultFuture.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Batch processing failed for groupId={}: {}", groupId, e.getMessage(), e);
            throw new InternalServerErrorException("Batch edge building failed");
        }
    }

    @Override
    public CompletableFuture<Void> indexNodes(List<Node> nodes, int page) {
        return null;
    }

    private void processNode(Node node, ProcessContext ctx) {
        logIfNeeded(node, nodeById.size(), ctx.getCounter());
        for (Node candidate : nodeById.values()) {
            if (isValidCandidate(node.getId(), candidate.getId())) {
                double score = 1.0; // Flat strategy assumes constant weight
                Edge edge = Edge.builder().fromNode(node).toNode(candidate).weight(score).build();
                ctx.getEdges().add(edge);
                GraphRecords.PotentialMatch match = new GraphRecords.PotentialMatch(
                        node.getReferenceId(),
                        candidate.getReferenceId(),
                        score,
                        ctx.getGroupId(),
                        ctx.getDomainId()
                );
                ctx.getMatches().add(match);
                ctx.getBatch().add(GraphRequestFactory.convertToPotentialMatch(match));
                flushIfNeeded(ctx);
            }
        }
    }

    private void logIfNeeded(Node node, int candidateCount, AtomicInteger counter) {
        if (counter.incrementAndGet() % LOG_THROTTLE_INTERVAL == 0) {
            log.info("Processed node id={}, candidates={}", node.getId(), candidateCount);
        }
    }

    private boolean isValidCandidate(UUID nodeId, UUID candidateId) {
        return nodeId.compareTo(candidateId) < 0;
    }

    private void flushIfNeeded(ProcessContext ctx) {
        if (ctx.getBatch().size() >= STORAGE_BATCH_SIZE) {
            List<PotentialMatchEntity> toSave = new ArrayList<>(ctx.getBatch());
            ctx.getBatch().clear();
            saver.saveMatchesAsync(toSave, ctx.getGroupId(), ctx.getDomainId(), "", false);
        }
    }
}