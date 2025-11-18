package com.shedule.x.processors;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.config.QueueManagerConfig;
import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.config.factory.QueueManagerFactory;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class PotentialMatchComputationProcessorImp implements PotentialMatchComputationProcessor {
    private final GraphStore graphStore;
    private final PotentialMatchSaver potentialMatchSaver;
    private final MeterRegistry meterRegistry;
    private final ExecutorService mappingExecutor;
    private final ExecutorService storageExecutor;
    private final ScheduledExecutorService watchdogExecutor;
    private final Semaphore saveSemaphore;
    private final QueueManagerFactory queueManagerFactory;
    private final QueueManagerConfig queueManagerConfig;

    private volatile boolean shutdownInitiated = false;
    private final AtomicInteger lastFlushIntervalSeconds = new AtomicInteger(0);

    @Value("${match.flush.min-interval-seconds:2}")
    private int minFlushIntervalSeconds;

    @Value("${match.flush.queue-threshold:0.8}")
    private double flushQueueThreshold;

    @Value("${match.save.timeout-seconds:300}")
    private int matchSaveTimeoutSeconds;

    @Value("${match.final-save.timeout-seconds:120}")
    private int finalSaveTimeoutSeconds;

    @Value("${match.temp-table.batch-size:200}")
    private int tempTableBatchSize;

    public PotentialMatchComputationProcessorImp(
            GraphStore graphStore,
            PotentialMatchSaver potentialMatchSaver,
            MeterRegistry meterRegistry,
            @Qualifier("persistenceExecutor") ExecutorService mappingExecutor,
            @Qualifier("matchesProcessExecutor") ExecutorService storageExecutor,
            @Qualifier("watchdogExecutor") ScheduledExecutorService watchdogExecutor,
            QueueManagerFactory queueManagerFactory,
            @Value("${match.semaphore.permits:8}") int semaphorePermits,
            @Value("${match.queue.capacity:1000000}") int queueCapacity,
            @Value("${match.flush.interval-seconds:5}") int flushIntervalSeconds,
            @Value("${match.queue.drain-warning-threshold:0.9}") double drainWarningThreshold,
            @Value("${match.boost-batch-factor:2}") int boostBatchFactor,
            @Value("${match.max-final-batch-size:50000}") int maxFinalBatchSize) {

        this.graphStore = Objects.requireNonNull(graphStore);
        this.potentialMatchSaver = Objects.requireNonNull(potentialMatchSaver);
        this.meterRegistry = Objects.requireNonNull(meterRegistry);
        this.mappingExecutor = mappingExecutor;
        this.storageExecutor = storageExecutor;
        this.watchdogExecutor = watchdogExecutor;
        this.saveSemaphore = new Semaphore(semaphorePermits, true);
        this.queueManagerFactory = queueManagerFactory;
        this.queueManagerConfig = new QueueManagerConfig(
                queueCapacity, flushIntervalSeconds, drainWarningThreshold, boostBatchFactor, maxFinalBatchSize);
        this.lastFlushIntervalSeconds.set(flushIntervalSeconds);
    }

    public CompletableFuture<Void> processChunkMatchesFallback(
            GraphRecords.ChunkResult chunkResult, UUID groupId, UUID domainId,
            String processingCycleId, int batchSize, Throwable throwable) {
        log.warn(
                "Circuit breaker triggered: Failed to process chunk matches for groupId={}, chunkIndex={}, processingCycleId={}: {}",
                groupId, chunkResult.getChunkIndex(), processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_circuit_breaker", "groupId", groupId.toString()).increment();
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> savePendingMatchesFallback(
            UUID groupId, UUID domainId, String processingCycleId, int batchSize, Throwable throwable) {
        log.warn("Fallback triggered: Failed to save pending matches for groupId={}, processingCycleId={}: {}",
                groupId, processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(), "error_type", "save")
                .increment();
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> saveFinalMatchesFallback(
            UUID groupId, UUID domainId, String processingCycleId, AutoCloseableStream<Edge> stream,
            Throwable throwable) {
        log.warn("Fallback triggered: Failed to save final matches for groupId={}, processingCycleId={}: {}",
                groupId, processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(), "error_type", "final_save")
                .increment();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public AutoCloseableStream<Edge> streamEdges(UUID groupId, UUID domainId, String processingCycleId, int topK) {
        Instant start = Instant.now();
        log.debug("Streaming edges for groupId={}, domainId={}, processingCycleId={}", groupId, domainId, processingCycleId);

        try {
            AutoCloseableStream<Edge> edgeStream = graphStore.streamEdges(domainId, groupId);
            long edgeCount = edgeStream.getStream().count();
            log.info("Streaming {} edges for groupId={}, domainId={}, processingCycleId={}", edgeCount, groupId, domainId, processingCycleId);

            AutoCloseableStream<Edge> resultStream = graphStore.streamEdges(domainId, groupId);
            return new AutoCloseableStream<>(resultStream.getStream().onClose(() -> {
                meterRegistry
                        .timer("match_processor_stream_latency", "groupId", groupId.toString(), "processingCycleId", processingCycleId)
                        .record(Duration.between(start, Instant.now()));
            }));
        } catch (Exception e) {
            log.error("Failed to stream edges for groupId={}, domainId={}, processingCycleId={}: {}", groupId, domainId, processingCycleId, e.getMessage());
            meterRegistry.counter("match_processor_stream_errors", "groupId", groupId.toString(), "processingCycleId", processingCycleId).increment();
            return new AutoCloseableStream<>(Stream.empty());
        }
    }

    public CompletableFuture<Void> saveMatchBatchFallback(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId,
            UUID domainId,
            String processingCycleId,
            int chunkIndex,
            Throwable throwable) {
        log.warn(
                "Fallback triggered: Failed to save match batch to GraphStore for groupId={}, processingCycleId={}: {}",
                groupId, processingCycleId, throwable.getMessage());
        meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(), "error_type", "save")
                .increment();
        cleanup(groupId);
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> savePendingMatchesBlocking(
            UUID groupId, UUID domainId, Integer batchSize, String processingCycleId) {
        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, processingCycleId,
                queueManagerConfig);
        QueueManagerImpl manager = queueManagerFactory.create(queueConfig);

        BlockingQueue<GraphRecords.PotentialMatch> queue = manager.getQueue();
        long startTime = System.currentTimeMillis();
        long totalProcessed = 0;
        int maxDrain = queueManagerConfig.getMaxFinalBatchSize();

        try {
            do {
                List<GraphRecords.PotentialMatch> batch = new ArrayList<>();
                int drained = queue.drainTo(batch, maxDrain);
                if (drained == 0) {
                    log.debug("No matches to drain for blocking save for groupId={}", groupId);
                    break;
                }

                log.info("Saving {} blocking matches to GraphStore for groupId={}", drained, groupId);
                totalProcessed += drained;

                CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                graphStore.persistEdgesAsync(batch, groupId, 0)
                        .orTimeout(matchSaveTimeoutSeconds, TimeUnit.SECONDS)
                        .thenAcceptAsync(result -> {
                            log.info(
                                    "Saved {} matches in blocking flush to GraphStore for groupId={}, processingCycleId={}",
                                    drained, groupId, processingCycleId);
                            meterRegistry
                                    .counter("match_saves_total", "groupId", groupId.toString(), "type", "blocking")
                                    .increment(drained);
                            resultFuture.complete(null);
                        }, mappingExecutor)
                        .exceptionally(throwable -> {
                            log.error("Blocking save to GraphStore failed for groupId={}, processingCycleId={}: {}",
                                    groupId, processingCycleId, throwable.getMessage());
                            meterRegistry.counter("match_processing_errors", "groupId", groupId.toString(),
                                    "error_type", "blocking_save").increment();
                            resultFuture.completeExceptionally(throwable);
                            return null;
                        });

                resultFuture.get(matchSaveTimeoutSeconds, TimeUnit.SECONDS);

                if (System.currentTimeMillis() - startTime > matchSaveTimeoutSeconds * 1000L) {
                    long remaining = queue.size();
                    log.warn("Shutdown limit of {} seconds reached, {} matches remaining for groupId={}",
                            matchSaveTimeoutSeconds, remaining, groupId);
                    meterRegistry.counter("matches_dropped_due_to_shutdown", "groupId", groupId.toString(), "domainId",
                            domainId.toString()).increment(remaining);
                    break;
                }
            } while (!queue.isEmpty());

            log.info(
                    "Completed blocking save to GraphStore for groupId={}, processed {} matches, remaining queue size={}",
                    groupId, totalProcessed, queue.size());
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            log.error("Blocking save to GraphStore failed for groupId={}: {}", groupId, e.getMessage());
            throw new CompletionException("Blocking save failed", e);
        }
    }


    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;
        mappingExecutor.shutdown();
        storageExecutor.shutdown();

        try {
            CompletableFuture<Void> flushFuture = CompletableFuture.runAsync(() -> {
                QueueManagerImpl.flushAllQueuesBlocking(this::savePendingMatchesBlocking);
                QueueManagerImpl.removeAll();
            });

            flushFuture.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Failed to flush queues during shutdown", e);
        }

        try {
            if (!mappingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                mappingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            if (!storageExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                storageExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Void> savePendingMatches(UUID groupId, UUID domainId, String processingCycleId, int batchSize) {
        if (shutdownInitiated) return CompletableFuture.completedFuture(null);

        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, processingCycleId, queueManagerConfig);
        QueueManagerImpl manager = queueManagerFactory.create(queueConfig);

        return processBatchIfNeeded(manager, null, batchSize, groupId, domainId, processingCycleId)
                .exceptionally(t -> {
                    log.warn("Failed to save pending matches for groupId={}", groupId, t);
                    return null;
                });
    }

    @Override
    public CompletableFuture<Void> saveFinalMatches(
            UUID groupId, UUID domainId, String processingCycleId,
            AutoCloseableStream<Edge> initialEdgeStream, int topK) {

        log.info("Starting FINAL SAVE with HARD CANCELLATION | groupId={}", groupId);

        AtomicBoolean semaphoreAcquired = new AtomicBoolean(false);
        AtomicReference<Future<?>> finalSaveTaskRef = new AtomicReference<>(null);

        CompletableFuture<Void> resultFuture = CompletableFuture.supplyAsync(() -> {
            try {
                if (!saveSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Final save backpressure: semaphore timeout");
                }
                semaphoreAcquired.set(true);
                log.debug("Acquired saveSemaphore for final save | groupId={}", groupId);

                Future<?> task = storageExecutor.submit(() -> {
                    Thread.currentThread().setName("final-save-" + groupId);
                    performFinalSaveWithInterruptionCheck(groupId, domainId, processingCycleId);
                });
                finalSaveTaskRef.set(task);

                // Soft timeout
                task.get(finalSaveTimeoutSeconds, TimeUnit.SECONDS);

                return null;

            } catch (TimeoutException e) {
                log.error("Final save SOFT TIMEOUT after {}s | groupId={}", finalSaveTimeoutSeconds, groupId);
                throw new RuntimeException("Final save exceeded soft timeout", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Final save interrupted", e);
            } catch (Exception e) {
                throw new RuntimeException("Final save failed", e);
            } finally {
                Future<?> task = finalSaveTaskRef.get();
                if (task != null && !task.isDone()) {
                    task.cancel(true);
                }
                if (semaphoreAcquired.get()) {
                    saveSemaphore.release();
                    log.debug("Released saveSemaphore after final save | groupId={}", groupId);
                }
            }
        }, storageExecutor);

        // Hard kill watchdog — now safe to capture
        watchdogExecutor.schedule(() -> {
            if (!resultFuture.isDone()) {
                log.error("HARD KILL: Final save exceeded {}s → killing thread | groupId={}",
                        finalSaveTimeoutSeconds * 2, groupId);
                meterRegistry.counter("final_save_hard_kill", "groupId", groupId.toString()).increment();

                Future<?> task = finalSaveTaskRef.get();
                if (task != null && !task.isDone()) {
                    task.cancel(true);
                }
                resultFuture.completeExceptionally(new TimeoutException("Final save hard timeout"));
            }
        }, finalSaveTimeoutSeconds * 2L, TimeUnit.SECONDS);

        return resultFuture.exceptionally(t -> {
            log.error("Final save failed for groupId={}: {}", groupId, t.getMessage());
            meterRegistry.counter("final_save_errors", "groupId", groupId.toString()).increment();
            return null;
        });
    }

    private void performFinalSaveWithInterruptionCheck(UUID groupId, UUID domainId, String processingCycleId) {
        AtomicLong processed = new AtomicLong(0);
        Map<String, List<PotentialMatchEntity>> matchesByRefId = new HashMap<>();
        List<PotentialMatchEntity> batch = new ArrayList<>(tempTableBatchSize);

        try (AutoCloseableStream<Edge> edgeStream = graphStore.streamEdges(domainId, groupId)) {
            edgeStream.forEach(edge -> {
                if (Thread.interrupted()) {
                    log.warn("Final save interrupted at {} edges | groupId={}", processed.get(), groupId);
                    throw new RuntimeException("Final save cancelled by interruption");
                }

                PotentialMatchEntity entity = GraphRequestFactory.convertToPotentialMatch(edge, groupId, domainId, processingCycleId);
                if (entity != null) {
                    matchesByRefId.computeIfAbsent(entity.getReferenceId(), k -> new ArrayList<>()).add(entity);
                }

                if (processed.incrementAndGet() % 10_000 == 0) {
                    Thread.yield();
                }
            });
        }

        List<CompletableFuture<Void>> saveFutures = new ArrayList<>();
        for (List<PotentialMatchEntity> matches : matchesByRefId.values()) {
            batch.addAll(matches);
            if (batch.size() >= tempTableBatchSize) {
                List<PotentialMatchEntity> batchToSave = new ArrayList<>(batch);
                saveFutures.add(potentialMatchSaver.saveMatchesAsync(batchToSave, groupId, domainId, processingCycleId, true));
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            saveFutures.add(potentialMatchSaver.saveMatchesAsync(batch, groupId, domainId, processingCycleId, true));
        }

        CompletableFuture.allOf(saveFutures.toArray(new CompletableFuture[0]))
                .join();

        log.info("Final save completed successfully | groupId={} | nodes={}", groupId, matchesByRefId.size());
    }

    @Override
    public CompletableFuture<Void> processChunkMatches(
            GraphRecords.ChunkResult chunkResult, UUID groupId, UUID domainId,
            String processingCycleId, int matchBatchSize) {

        if (shutdownInitiated) return CompletableFuture.completedFuture(null);

        QueueConfig queueConfig = GraphRequestFactory.getQueueConfig(groupId, domainId, processingCycleId, queueManagerConfig);
        QueueManagerImpl manager = queueManagerFactory.create(queueConfig);
        BlockingQueue<GraphRecords.PotentialMatch> queue = manager.getQueue();

        List<GraphRecords.PotentialMatch> matches = chunkResult.getMatches();
        if (matches == null || matches.isEmpty()) return CompletableFuture.completedFuture(null);

        return CompletableFuture.runAsync(() -> {
            int offered = 0;
            for (GraphRecords.PotentialMatch match : matches) {
                if (Thread.interrupted()) throw new RuntimeException("Chunk processing interrupted");

                try {
                    if (queue.offer(match, 100, TimeUnit.MILLISECONDS)) {
                        offered++;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while offering match", e);
                }
            }

            double load = (double) queue.size() / queueManagerConfig.getCapacity();
            if (load > flushQueueThreshold) {
                int newInterval = Math.max(minFlushIntervalSeconds, queueManagerConfig.getFlushIntervalSeconds() / 5);
                if (lastFlushIntervalSeconds.compareAndSet(lastFlushIntervalSeconds.get(), newInterval)) {
                    manager.setFlushInterval(newInterval);
                }
            }

            if (offered > 0) {
                processBatchIfNeeded(manager, chunkResult, matchBatchSize, groupId, domainId, processingCycleId);
            }
        }, mappingExecutor);
    }

    private CompletableFuture<Void> processBatchIfNeeded(QueueManagerImpl manager, GraphRecords.ChunkResult chunkResult,
                                                         int batchSize, UUID groupId, UUID domainId, String processingCycleId) {
        BlockingQueue<GraphRecords.PotentialMatch> queue = manager.getQueue();
        List<GraphRecords.PotentialMatch> batch = new ArrayList<>();
        int drained = queue.drainTo(batch, batchSize);

        if (drained > 0) {
            return saveMatchBatch(batch, groupId, domainId, processingCycleId,
                    chunkResult != null ? chunkResult.getChunkIndex() : -1);
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> saveMatchBatch(List<GraphRecords.PotentialMatch> matches,
                                                   UUID groupId, UUID domainId, String processingCycleId, int chunkIndex) {
        List<PotentialMatchEntity> entities = matches.stream()
                .map(GraphRequestFactory::convertToPotentialMatch)
                .filter(Objects::nonNull)
                .toList();

        CompletableFuture<Void> dbFuture = potentialMatchSaver.saveMatchesAsync(entities, groupId, domainId, processingCycleId, false)
                .exceptionally(t -> { log.warn("DB save failed for groupId={}", groupId, t); return null; });

        CompletableFuture<Void> graphStoreFuture = graphStore.persistEdgesAsync(matches, groupId, chunkIndex)
                .exceptionally(t -> { log.warn("GraphStore save failed for groupId={}", groupId, t); return null; });

        return CompletableFuture.allOf(dbFuture, graphStoreFuture);
    }

    @Override
    public void cleanup(UUID groupId) {
        try {
            QueueManagerImpl.flushQueueBlocking(groupId, this::savePendingMatchesBlocking);
            QueueManagerImpl.remove(groupId);
            potentialMatchSaver.deleteByGroupId(groupId);
            log.info("Cleanup completed for groupId={}", groupId);
        } catch (Exception e) {
            log.error("Cleanup failed for groupId={}", groupId, e);
        }
    }

    @Override
    public long getFinalMatchCount(UUID groupId, UUID domainId, String processingCycleId) {
        try (AutoCloseableStream<Edge> stream = graphStore.streamEdges(domainId, groupId)) {
            long count = stream.getStream().count();
            graphStore.cleanEdges(groupId);
            return count;
        } catch (Exception e) {
            log.error("Failed to count final matches for groupId={}", groupId, e);
            return 0;
        }
    }
}