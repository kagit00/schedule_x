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
        int batchSize = matches.size();
        long timeoutSeconds = Math.min(300 + (batchSize / 100), 1800); // Adaptive: 5min + overhead, max 30min

        log.debug("Saving batch of {} matches with timeout {}s for groupId={}",
                batchSize, timeoutSeconds, groupId);

        List<PotentialMatchEntity> entities = matches.parallelStream()
                .map(GraphRequestFactory::convertToPotentialMatch)
                .filter(Objects::nonNull)
                .toList();

        CompletableFuture<Void> dbFuture = potentialMatchSaver
                .saveMatchesAsync(entities, groupId, domainId, processingCycleId, false)
                .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(t -> {
                    Throwable rootCause = unwrapCompletionException(t);
                    String errorType = rootCause instanceof TimeoutException ? "timeout" : "error";

                    log.error("DB save {} for groupId={}: {}", errorType, groupId, rootCause.getMessage());
                    meterRegistry.counter("match.batch.db_save.failure",
                            "error_type", errorType).increment();
                    return null;
                });

        CompletableFuture<Void> graphFuture = persistEdgesWithRetry(
                matches, groupId, chunkIndex, processingCycleId,
                5, 2000, timeoutSeconds);

        return CompletableFuture.allOf(dbFuture, graphFuture)
                .whenComplete((result, error) -> {
                    long durationMs = Duration.between(start, Instant.now()).toMillis();

                    if (error != null) {
                        Throwable rootCause = unwrapCompletionException(error);
                        String errorType = rootCause instanceof TimeoutException ? "timeout" : "error";

                        log.error("Batch save {} | groupId={} | {} matches | {}ms | {}",
                                errorType, groupId, batchSize, durationMs, rootCause.getMessage());

                        meterRegistry.timer("match.batch.save.time",
                                        "status", "failed", "error_type", errorType)
                                .record(durationMs, TimeUnit.MILLISECONDS);
                    } else {
                        log.debug("Batch saved successfully | groupId={} | {} matches | {}ms",
                                groupId, batchSize, durationMs);

                        meterRegistry.timer("match.batch.save.time", "status", "success")
                                .record(durationMs, TimeUnit.MILLISECONDS);
                    }
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

    private CompletableFuture<Void> persistEdgesWithRetry(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId, int chunkIndex, String processingCycleId,
            int maxRetries, long initialDelayMs, long timeoutSeconds) {

        return persistEdgesWithRetryInternal(matches, groupId, chunkIndex, processingCycleId,
                maxRetries, initialDelayMs, timeoutSeconds, 1);
    }

    private CompletableFuture<Void> persistEdgesWithRetryInternal(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId, int chunkIndex, String processingCycleId,
            int maxRetries, long baseDelayMs, long timeoutSeconds, int attempt) {

        return graphStore.persistEdgesAsync(matches, groupId, chunkIndex, processingCycleId)
                .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                .exceptionallyCompose(error -> {
                    Throwable rootCause = unwrapCompletionException(error);

                    if (attempt < maxRetries && isRetriableError(rootCause)) {
                        long delayMs = baseDelayMs * (1L << (attempt - 1));

                        log.warn("LMDB persist failed (attempt {}/{}), retrying in {}ms for groupId={}: {}",
                                attempt, maxRetries, delayMs, groupId, rootCause.getMessage());

                        meterRegistry.counter("match.batch.lmdb_save.retry",
                                "attempt", String.valueOf(attempt),
                                "groupId", groupId.toString()).increment();

                        return CompletableFuture
                                .runAsync(() -> {
                                    try {
                                        Thread.sleep(delayMs);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                        throw new CompletionException(ie);
                                    }
                                }, mappingExecutor)
                                .thenCompose(v -> persistEdgesWithRetryInternal(
                                        matches, groupId, chunkIndex, processingCycleId,
                                        maxRetries, baseDelayMs, timeoutSeconds, attempt + 1
                                ));
                    } else {
                        String errorType = rootCause instanceof TimeoutException ? "timeout" : "error";

                        log.error("LMDB persist {} permanently after {} attempts for groupId={}: {}",
                                errorType, attempt, groupId, rootCause.getMessage());

                        meterRegistry.counter("match.batch.lmdb_save.failure",
                                "groupId", groupId.toString(),
                                "error_type", errorType).increment();

                        return CompletableFuture.failedFuture(
                                new CompletionException("LMDB persist failed after retries", rootCause)
                        );
                    }
                });
    }

    private boolean isRetriableError(Throwable t) {
        if (t instanceof TimeoutException) {
            return true;
        }

        String msg = t.getMessage();
        return msg != null && (
                msg.contains("Write queue is full") ||
                        msg.contains("temporarily unavailable") ||
                        msg.contains("lock")
        );
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

    @Override
    public void cleanup(UUID groupId) {
        QueueManagerImpl.remove(groupId);
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
        log.info("Starting final save for groupId={}", groupId);

        List<CompletableFuture<Void>> saveFutures = new ArrayList<>();
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
                    List<PotentialMatchEntity> batchToSave = new ArrayList<>(buffer);
                    CompletableFuture<Void> saveFuture = flushFinalBatchAsync(
                            batchToSave, groupId, domainId, processingCycleId);
                    saveFutures.add(saveFuture);

                    buffer.clear();

                    if (saveFutures.size() >= 10) {
                        CompletableFuture.allOf(saveFutures.toArray(new CompletableFuture[0])).join();
                        saveFutures.clear();
                    }
                }

                totalProcessed++;
                if (totalProcessed % 1_000_000 == 0) { // Log every 1M for 50M
                    log.info("Final save progress: {}M items | groupId={}", totalProcessed / 1_000_000, groupId);
                }
            }

            // Flush remaining
            if (!buffer.isEmpty()) {
                saveFutures.add(flushFinalBatchAsync(buffer, groupId, domainId, processingCycleId));
            }

            // Wait for all saves to complete
            if (!saveFutures.isEmpty()) {
                CompletableFuture.allOf(saveFutures.toArray(new CompletableFuture[0])).join();
            }

            log.info("Cleaning up edges for groupId={}, cycle={}", groupId, processingCycleId);
            //graphStore.cleanEdges(groupId, processingCycleId);

            log.info("Final save completed. Total: {} | groupId={}", totalProcessed, groupId);

        } catch (Exception e) {
            log.error("Critical error during final save for groupId={}", groupId, e);
            meterRegistry.counter("final_save.critical_error", "groupId", groupId.toString()).increment();
            throw new CompletionException("Final save failed", e);
        }
    }

    private CompletableFuture<Void> flushFinalBatchAsync(
            List<PotentialMatchEntity> batch, UUID groupId, UUID domainId, String cycleId) {

        return potentialMatchSaver.saveMatchesAsync(batch, groupId, domainId, cycleId, true)
                .orTimeout(matchSaveTimeoutSeconds, TimeUnit.SECONDS)
                .exceptionally(t -> {
                    log.error("Final batch save failed for groupId={}: {}", groupId, t.getMessage());
                    throw new CompletionException("Final batch save failed", t);
                });
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
                log.info("Finished draining | groupId={} | Total={}", groupId, totalDrainedSoFar);
            }
            return CompletableFuture.completedFuture(null);
        }

        if (shouldApplyBackpressure(groupId)) {
            log.warn("Applying backpressure for groupId={} (system overloaded, pausing 2s)", groupId);
            meterRegistry.counter("drain.backpressure_applied", "groupId", groupId.toString()).increment();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return saveMatchBatch(batch, groupId, domainId, processingCycleId, -1)
                .thenComposeAsync(v -> {
                    long newTotal = totalDrainedSoFar + batch.size();

                    if (newTotal % 100_000 == 0) {
                        log.info("Drain progress: {} | groupId={}", newTotal, groupId);
                    }

                    return drainAndSaveRecursively(
                            manager, groupId, domainId, processingCycleId,
                            batchSize, newTotal
                    );
                }, mappingExecutor)
                .exceptionally(t -> {
                    Throwable rootCause = unwrapCompletionException(t);
                    log.error("Drain failed for groupId={}: {}",
                            groupId, rootCause.getMessage(), rootCause);
                    throw new CompletionException(rootCause);
                });
    }

    private boolean shouldApplyBackpressure(UUID groupId) {
        int availablePermits = saveSemaphore.availablePermits();

        if (availablePermits < 4) {
            log.debug("Semaphore pressure detected: only {} permits available", availablePermits);
            return true;
        }

        // access EdgePersistence queue size, check that too

        return false;
    }

    private Throwable unwrapCompletionException(Throwable t) {
        Throwable current = t;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }


    @PreDestroy
    public void shutdown() {
        shutdownInitiated = true;
        log.info("Shutting down PotentialMatchComputationProcessor...");

        try {
            log.info("Flushing all pending queues...");
            QueueManagerImpl.flushAllQueuesAsync(this::savePendingMatchesAsync)
                    .get(5, TimeUnit.MINUTES);
            log.info("All queues flushed successfully");

        } catch (TimeoutException e) {
            log.error("Queue flush timed out after 5 minutes");
            meterRegistry.counter("shutdown.flush_timeout").increment();
        } catch (Exception e) {
            log.error("Error flushing queues during shutdown", e);
            meterRegistry.counter("shutdown.flush_error").increment();
        }

        shutdownExecutor(mappingExecutor, "mappingExecutor", 30, TimeUnit.SECONDS);
        shutdownExecutor(storageExecutor, "storageExecutor", 30, TimeUnit.SECONDS);
        shutdownExecutor(watchdogExecutor, "watchdogExecutor", 10, TimeUnit.SECONDS);

        log.info("PotentialMatchComputationProcessor shutdown complete");
    }

    private void shutdownExecutor(ExecutorService executor, String name, long timeout, TimeUnit unit) {
        try {
            log.info("Shutting down {}...", name);
            executor.shutdown();

            if (!executor.awaitTermination(timeout, unit)) {
                log.warn("{} did not terminate gracefully, forcing shutdown", name);
                executor.shutdownNow();

                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error("{} did not terminate after forced shutdown", name);
                }
            }

            log.info("{} shutdown complete", name);

        } catch (InterruptedException e) {
            log.error("{} shutdown interrupted", name, e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}