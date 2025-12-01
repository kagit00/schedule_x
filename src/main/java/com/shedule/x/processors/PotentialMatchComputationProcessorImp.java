package com.shedule.x.processors;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.config.QueueManagerConfig;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.config.factory.QueueManagerFactory;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.models.Edge;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import io.micrometer.core.instrument.Timer;
import java.util.*;
import java.util.concurrent.*;


@Slf4j
@Component
public class PotentialMatchComputationProcessorImp implements PotentialMatchComputationProcessor {

    private final GraphStore graphStore;
    private final PotentialMatchSaver potentialMatchSaver;
    private final MeterRegistry meterRegistry;

    private final ExecutorService mappingExecutor;
    private final ExecutorService storageExecutor;
    private final ScheduledExecutorService watchdogExecutor;

    private final QueueManagerFactory queueManagerFactory;
    private final QueueManagerConfig baseQueueConfig;
    private final Semaphore saveSemaphore;

    private volatile boolean shutdownInitiated = false;

    @Value("${match.save.timeout-seconds:300}")
    private int matchSaveTimeoutSeconds;

    @Value("${match.final-save.batch-size:2000}")
    private int finalSaveBatchSize;

    public PotentialMatchComputationProcessorImp(
            GraphStore graphStore,
            PotentialMatchSaver potentialMatchSaver,
            MeterRegistry meterRegistry,
            @Qualifier("persistenceExecutor") ExecutorService mappingExecutor,
            @Qualifier("matchesProcessExecutor") ExecutorService storageExecutor,
            @Qualifier("watchdogExecutor") ScheduledExecutorService watchdogExecutor,
            QueueManagerFactory queueManagerFactory,
            @Value("${match.semaphore.permits:16}") int semaphorePermits,
            @Value("${match.queue.capacity:1000000}") int queueCapacity,
            @Value("${match.flush.interval-seconds:5}") int flushIntervalSeconds,
            @Value("${match.queue.drain-warning-threshold:0.8}") double drainWarningThreshold,
            @Value("${match.max-final-batch-size:50000}") int maxFinalBatchSize) {

        this.graphStore = Objects.requireNonNull(graphStore);
        this.potentialMatchSaver = Objects.requireNonNull(potentialMatchSaver);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        this.mappingExecutor = mappingExecutor;
        this.storageExecutor = storageExecutor;
        this.watchdogExecutor = watchdogExecutor;
        this.queueManagerFactory = queueManagerFactory;
        this.saveSemaphore = new Semaphore(semaphorePermits);

        this.baseQueueConfig = new QueueManagerConfig(
                queueCapacity,
                flushIntervalSeconds,
                drainWarningThreshold,
                2,
                maxFinalBatchSize,
                true
        );
    }


    @Override
    public CompletableFuture<Void> processChunkMatches(
            GraphRecords.ChunkResult chunkResult, UUID groupId, UUID domainId,
            String processingCycleId, int matchBatchSize) {

        if (shutdownInitiated) return CompletableFuture.completedFuture(null);
        List<GraphRecords.PotentialMatch> matches = chunkResult.getMatches();
        if (matches == null || matches.isEmpty()) return CompletableFuture.completedFuture(null);

        return CompletableFuture.runAsync(() -> {
            QueueManagerImpl manager = getOrCreateQueueManager(groupId, domainId, processingCycleId);

            for (GraphRecords.PotentialMatch match : matches) {
                boolean accepted = manager.enqueue(match);
                if (!accepted) {
                    meterRegistry.counter("queue.rejected", "groupId", groupId.toString()).increment();
                }
            }

        }, mappingExecutor).exceptionally(t -> {
            log.error("Chunk ingest failed for groupId={}", groupId, t);
            return null;
        });
    }


    private CompletableFuture<Void> saveMatchBatch(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId, UUID domainId, String processingCycleId, int chunkIndex) {

        Instant start = Instant.now();

        List<PotentialMatchEntity> entities = matches.parallelStream()
                .map(GraphRequestFactory::convertToPotentialMatch)
                .filter(Objects::nonNull)
                .toList();

        CompletableFuture<Void> dbFuture = potentialMatchSaver.saveMatchesAsync(entities, groupId, domainId, processingCycleId, false)
                .orTimeout(matchSaveTimeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(t -> {
                    log.error("DB Save failed (non-fatal) for groupId={}: {}", groupId, t.getMessage());
                    return null;
                });

        CompletableFuture<Void> graphFuture = graphStore.persistEdgesAsync(matches, groupId, chunkIndex, processingCycleId)
                .orTimeout(matchSaveTimeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(t -> {
                    throw new CompletionException("LMDB persist failed", t);
                });

        return CompletableFuture.allOf(dbFuture, graphFuture)
                .thenRun(() -> {
                    Timer.builder("match.batch.save.time")
                            .tag("groupId", groupId.toString())
                            .register(meterRegistry)
                            .record(Duration.between(start, Instant.now()));
                });
    }

    @Override
    public CompletableFuture<Void> saveFinalMatches(
            UUID groupId, UUID domainId, String processingCycleId,
            AutoCloseableStream<Edge> initialEdgeStream, int topK) {

        log.info("Starting FINAL SAVE for groupId={}", groupId);

        return CompletableFuture.runAsync(() -> {
            performStreamingFinalSave(groupId, domainId, processingCycleId);
        }, storageExecutor).exceptionally(t -> {
            log.error("Final save failed for groupId={}", groupId, t);
            meterRegistry.counter("final_save.error", "groupId", groupId.toString()).increment();
            return null;
        });
    }

    private QueueManagerImpl getOrCreateQueueManager(UUID groupId, UUID domainId, String processingCycleId) {
        QueueConfig requestConfig = new QueueConfig(
                groupId, domainId, processingCycleId,
                baseQueueConfig.capacity(),
                baseQueueConfig.flushIntervalSeconds(),
                baseQueueConfig.drainWarningThreshold(),
                baseQueueConfig.boostBatchFactor(),
                baseQueueConfig.maxFinalBatchSize(),
                baseQueueConfig.useDiskSpill()
        );

        return queueManagerFactory.create(requestConfig);
    }

    private CompletableFuture<Void> acquireSemaphore(UUID groupId) {
        return CompletableFuture.runAsync(() -> {
            try {
                if (!saveSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                    throw new TimeoutException("Could not acquire save semaphore for groupId=" + groupId);
                }
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, mappingExecutor);
    }

    @Override
    public void cleanup(UUID groupId) {
        QueueManagerImpl.remove(groupId);
    }

    @PreDestroy
    public void shutdown() {
        shutdownInitiated = true;
        log.info("Shutting down Processor...");

        QueueManagerImpl.flushAllQueuesAsync(this::savePendingMatchesAsync).join();

        mappingExecutor.shutdown();
        storageExecutor.shutdown();
    }

    @Override
    public AutoCloseableStream<EdgeDTO> streamEdges(UUID groupId, UUID domainId, String processingCycleId, int topK) {
        return graphStore.streamEdges(domainId, groupId, processingCycleId);
    }


    @Override
    public long getFinalMatchCount(UUID groupId, UUID domainId, String processingCycleId) {
        try (AutoCloseableStream<EdgeDTO> stream = graphStore.streamEdges(domainId, groupId, processingCycleId)) {
            return stream.getStream().count();
        } catch (Exception e) {
            log.error("Failed to count matches for groupId={}", groupId, e);
            return 0;
        }
    }


    private void performStreamingFinalSave(UUID groupId, UUID domainId, String processingCycleId) {
        List<PotentialMatchEntity> buffer = new ArrayList<>(finalSaveBatchSize);
        long totalProcessed = 0;

        try (AutoCloseableStream<EdgeDTO> edgeStream = graphStore.streamEdges(domainId, groupId, processingCycleId)) {
            Iterator<EdgeDTO> iterator = edgeStream.getStream().iterator();

            while (iterator.hasNext()) {
                EdgeDTO edge = iterator.next();
                PotentialMatchEntity entity = GraphRequestFactory.convertToPotentialMatch(
                        edge, groupId, domainId, processingCycleId);

                if (entity != null) {
                    buffer.add(entity);
                }

                if (buffer.size() >= finalSaveBatchSize) {
                    flushFinalBatch(buffer, groupId, domainId, processingCycleId);
                }

                totalProcessed++;
                if (totalProcessed % 50_000 == 0) {
                    log.info("Final save progress: {} items | groupId={}", totalProcessed, groupId);
                }
            }

            flushFinalBatch(buffer, groupId, domainId, processingCycleId);
            graphStore.cleanEdges(groupId, processingCycleId);
            log.info("Final save completed. Total: {} | groupId={}", totalProcessed, groupId);

        } catch (Exception e) {
            log.error("Critical error during final save for groupId={}", groupId, e);
            throw new CompletionException("Final save failed", e);
        }
    }

    private void flushFinalBatch(List<PotentialMatchEntity> buffer, UUID groupId, UUID domainId, String cycleId) {
        if (buffer.isEmpty()) return;
        try {
            potentialMatchSaver.saveMatchesAsync(new ArrayList<>(buffer), groupId, domainId, cycleId, true)
                    .get(matchSaveTimeoutSeconds, TimeUnit.SECONDS);
            buffer.clear();
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush final batch to DB", e);
        }
    }

    @Override
    public CompletableFuture<Void> savePendingMatchesAsync(UUID groupId, UUID domainId, String processingCycleId, int batchSize) {
        QueueManagerImpl manager = QueueManagerImpl.getExisting(groupId);

        if (manager == null) {
            return CompletableFuture.completedFuture(null);
        }

        //log.info("Starting drain of pending matches for groupId={} | Queue Memory={} | Spill Size={}", groupId, manager.getQueueSize(), manager.getDiskSpillSize());
        return drainAndSaveRecursively(manager, groupId, domainId, processingCycleId, batchSize, 0);
    }

    private CompletableFuture<Void> drainAndSaveRecursively(
            QueueManagerImpl manager,
            UUID groupId,
            UUID domainId,
            String processingCycleId,
            int batchSize,
            long totalDrainedSoFar) {

        List<GraphRecords.PotentialMatch> batch = manager.drainBatch(batchSize);

        if (batch.isEmpty()) {
            if (totalDrainedSoFar > 0) {
                log.info("Finished draining pending matches | groupId={} | Total Drained={}", groupId, totalDrainedSoFar);
            }
            return CompletableFuture.completedFuture(null);
        }

        return saveMatchBatch(batch, groupId, domainId, processingCycleId, -1)
                .thenComposeAsync(v ->
                                drainAndSaveRecursively(
                                        manager,
                                        groupId,
                                        domainId,
                                        processingCycleId,
                                        batchSize,
                                        totalDrainedSoFar + batch.size()
                                ),
                        mappingExecutor
                )
                .exceptionally(t -> {
                    log.error("Error during recursive drain for groupId={}", groupId, t);
                    throw new CompletionException(t);
                });
    }
}