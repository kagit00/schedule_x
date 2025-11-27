package com.shedule.x.builder;

import com.shedule.x.config.EdgeBuildingConfig;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.dto.Snapshot;
import com.shedule.x.dto.enums.State;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Node;
import com.shedule.x.processors.EdgeProcessor;
import com.shedule.x.processors.LSHIndex;
import com.shedule.x.processors.MetadataCompatibilityCalculator;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.processors.MetadataEncoder;
import com.shedule.x.service.NodeDataService;
import com.shedule.x.utils.basic.IndexUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Getter
public class MetadataEdgeBuildingStrategy implements SymmetricEdgeBuildingStrategy {
    private final Map<UUID, Long> lastModified = new ConcurrentHashMap<>();
    private final AtomicReference<State> prepState = new AtomicReference<>(State.UNINITIALIZED);
    private final AtomicReference<CompletableFuture<Boolean>> preparationFuture =
            new AtomicReference<>(CompletableFuture.completedFuture(false));
    private final Semaphore chunkSemaphore = new Semaphore(4, true);
    private final EdgeBuildingConfig config;
    private final EdgeProcessor edgeProcessor;
    private final LSHIndex lshIndex;
    private final CompatibilityCalculator compatibilityCalculator;
    private final MetadataEncoder metadataEncoder;
    private final ExecutorService executor;
    private final AtomicReference<Snapshot> currentSnapshot = new AtomicReference<>(new Snapshot(Map.of()));
    private volatile Throwable preparationFailureCause = null;
    private final ThreadLocal<List<GraphRecords.PotentialMatch>> chunkMatchesBuffer =
            ThreadLocal.withInitial(() -> new ArrayList<>(32));
    private final NodeDataService nodeDataService;

    public MetadataEdgeBuildingStrategy(EdgeBuildingConfig config, LSHIndex lshIndex,
                                        MetadataEncoder metadataEncoder, ExecutorService executor, EdgeProcessor edgeProcessor,
                                        NodeDataService nodeDataService) {
        this.config = config;
        this.lshIndex = lshIndex;
        this.compatibilityCalculator = new MetadataCompatibilityCalculator();
        this.metadataEncoder = metadataEncoder;
        this.executor = executor;
        this.edgeProcessor = edgeProcessor;
        this.nodeDataService = nodeDataService;
        log.info("Initialized MetadataEdgeBuildingStrategy (similarityThreshold={}, candidateLimit={}).",
                config.getSimilarityThreshold(), config.getCandidateLimit());
    }

    @Override
    public void processBatch(
            List<NodeDTO> sourceNodes,
            List<NodeDTO> targetNodes,
            Collection<GraphRecords.PotentialMatch> matches,
            Set<Edge> edges,
            MatchingRequest request,
            Map<String, Object> context) {

        UUID groupId = request.getGroupId();
        int targetSize = (targetNodes == null) ? 0 : targetNodes.size();
        log.info("Processing batch: Source={} vs Target={} for groupId={}", sourceNodes.size(), targetSize, groupId);

        boolean acquired = false;
        try {
            acquired = chunkSemaphore.tryAcquire(config.getChunkTimeoutSeconds(), TimeUnit.SECONDS);
            if (!acquired) {
                log.warn("Timed out acquiring chunkSemaphore for groupId={}", groupId);
                return;
            }

            matches.addAll(edgeProcessor.processBatchSync(
                    sourceNodes,
                    targetNodes,
                    groupId,
                    request.getDomainId(),
                    lshIndex,
                    compatibilityCalculator,
                    metadataEncoder,
                    currentSnapshot,
                    config,
                    chunkSemaphore,
                    executor,
                    chunkMatchesBuffer
            ));

            log.info("Completed batch for groupId={}, total matches found in this task={}",
                    groupId, matches.size());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted acquiring chunkSemaphore for groupId={}", groupId, e);
        } catch (Exception e) {
            log.error("Batch processing failed for groupId={}", groupId, e);
            throw new RuntimeException("Batch processing failed for groupId=" + groupId, e);
        } finally {
            if (acquired) {
                chunkSemaphore.release();
            }
        }
    }

    @Override
    public CompletableFuture<Void> indexNodes(List<NodeDTO> nodes, int page) {
        log.info("Indexing {} nodes for page={}", nodes.size(), page);
        preparationFailureCause = null;

        CompletableFuture<Void> newFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> newPrep = newFuture.thenApply(v -> {
            prepState.set(State.SUCCESS);
            return true;
        }).exceptionally(e -> {
            prepState.set(State.FAILED);
            preparationFailureCause = e;
            log.error("LSH preparation failed for page={}", page, e);
            return false;
        });

        if (!prepState.compareAndSet(State.UNINITIALIZED, State.IN_FLIGHT) &&
                !prepState.compareAndSet(State.FAILED, State.IN_FLIGHT)) {
            log.info("Joining existing indexing operation for page={}", page);
            return preparationFuture.get().thenApply(v -> null);
        }

        preparationFuture.set(newPrep);

        return IndexUtils.indexNodes(
                nodes,
                page,
                lshIndex,
                metadataEncoder,
                lastModified,
                currentSnapshot,
                prepState,
                preparationFuture,
                newFuture,
                config.getMaxRetries(),
                config.getRetryDelayMillis(),
                nodeDataService
        );
    }
}