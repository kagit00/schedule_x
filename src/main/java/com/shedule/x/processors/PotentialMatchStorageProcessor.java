package com.shedule.x.processors;

import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.utils.db.BatchUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import static com.shedule.x.utils.basic.Constant.*;


@Slf4j
@Component
public class PotentialMatchStorageProcessor {
    private static final long SAVE_TIMEOUT_MS = 300_000;
    private static final int MAX_CONCURRENT_SAVES = 12;

    private final HikariDataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final ExecutorService storageExecutor;
    private final Semaphore storageSemaphore;
    private volatile boolean shutdownInitiated = false;

    @Value("${matches.save.batch-size:5000}")
    private int batchSize;

    public PotentialMatchStorageProcessor(
            HikariDataSource dataSource,
            MeterRegistry meterRegistry,
            @Qualifier("matchesStorageExecutor") ExecutorService storageExecutor
    ) {
        this.dataSource = dataSource;
        this.meterRegistry = meterRegistry;
        this.storageExecutor = storageExecutor;
        this.storageSemaphore = new Semaphore(MAX_CONCURRENT_SAVES);
        meterRegistry.gauge(
                "system_cpu_usage",
                ManagementFactory.getOperatingSystemMXBean(),
                OperatingSystemMXBean::getSystemLoadAverage
        );
    }

    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;
        try {
            storageExecutor.shutdown();
            if (!storageExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
                storageExecutor.shutdownNow();
            }
            dataSource.close();
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    public CompletableFuture<Void> savePotentialMatches(List<PotentialMatchEntity> matches, String groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Save aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("PotentialMatchStorageProcessor is shutting down"));
        }

        if (matches.isEmpty()) {
            log.debug("No matches to save for groupId={}", groupId);
            return CompletableFuture.completedFuture(null);
        }

        List<PotentialMatchEntity> safeMatches = new ArrayList<>(matches);
        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Queueing save of {} potential matches for groupId={}, domainId={}, processingCycleId={}",
                safeMatches.size(), groupId, domainId, processingCycleId);

        try {
            if (!storageSemaphore.tryAcquire(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                log.warn("Timeout acquiring storageSemaphore for groupId={}", groupId);
                meterRegistry.counter("semaphore_acquire_timeout", "groupId", groupId).increment();
                return CompletableFuture.failedFuture(
                        new TimeoutException("Failed to acquire storage semaphore within timeout"));
            }
        } catch (InterruptedException e) {
            log.error("Semaphore acquisition interrupted for groupId={}", groupId);
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }

        return CompletableFuture.runAsync(() -> saveInBatches(safeMatches, groupId, domainId, processingCycleId), storageExecutor)
                .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .whenComplete((result, throwable) -> {
                    storageSemaphore.release();
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                            meterRegistry.timer("storage_processor_duration", "groupId", groupId)));
                    if (throwable != null) {
                        meterRegistry.counter("storage_processor_errors", "groupId", groupId, "error_type", "save")
                                .increment(safeMatches.size());
                        log.error("Failed to save {} matches for groupId={}, processingCycleId={}: {}",
                                safeMatches.size(), groupId, processingCycleId, throwable.getMessage());
                    } else {
                        meterRegistry.counter("storage_processor_matches_saved_total", "groupId", groupId)
                                .increment(safeMatches.size());
                        log.info("Saved {} potential matches for groupId={} in {} ms", safeMatches.size(), groupId, durationMs);
                    }
                });
    }

    public long countFinalMatches(String groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Count final matches aborted for groupId={} due to shutdown", groupId);
            throw new IllegalStateException("PotentialMatchStorageProcessor is shutting down");
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        log.debug("Counting final matches for groupId={}, domainId={}, processingCycleId={}", groupId, domainId, processingCycleId);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM public.potential_matches " +
                             "WHERE group_id = ? AND domain_id = ? AND processing_cycle_id = ?"
             )) {
            stmt.setString(1, groupId);
            stmt.setObject(2, domainId);
            stmt.setString(3, processingCycleId);
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                            meterRegistry.timer("storage_processor_count_duration", "groupId", groupId)));
                    log.debug("Counted {} final matches for groupId={} in {} ms", count, groupId, durationMs);
                    return count;
                }
                return 0;
            }
        } catch (SQLException e) {
            meterRegistry.counter("storage_processor_errors", "groupId", groupId, "error_type", "count").increment();
            log.error("Failed to count final matches for groupId={}, processingCycleId={}: {}",
                    groupId, processingCycleId, e.getMessage());
            throw new CompletionException("Count final matches failed", e);
        }
    }

    @Retryable(value = {SQLException.class, TimeoutException.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000, multiplier = 2))
    public CompletableFuture<Void> deleteByGroupId(String groupId) {
        if (shutdownInitiated) {
            log.warn("Delete aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("PotentialMatchStorageProcessor is shutting down"));
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Deleting potential matches for groupId={}", groupId);
        return CompletableFuture.runAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement("DELETE FROM public.potential_matches WHERE group_id = ?")) {
                stmt.setString(1, groupId);
                int rows = stmt.executeUpdate();
                long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                        meterRegistry.timer("storage_processor_delete_duration", "groupId", groupId)));
                meterRegistry.counter("storage_processor_matches_deleted_total", "groupId", groupId).increment(rows);
                log.info("Deleted {} matches for groupId={} in {} ms", rows, groupId, durationMs);
            } catch (SQLException e) {
                meterRegistry.counter("storage_processor_errors", "groupId", groupId, "error_type", "delete").increment();
                log.error("Failed to delete matches for groupId={}: {}", groupId, e.getMessage());
                throw new CompletionException("Delete failed", e);
            }
        }, storageExecutor);
    }

    private void saveInBatches(List<PotentialMatchEntity> matches, String groupId, UUID domainId, String processingCycleId) {
        List<List<PotentialMatchEntity>> batches = BatchUtils.partition(matches, batchSize);
        for (List<PotentialMatchEntity> batch : batches) {
            saveBatch(batch, groupId, domainId, processingCycleId);
        }
    }

    @Transactional
    @Retryable(value = {SQLException.class, TimeoutException.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000, multiplier = 2))
    private void saveBatch(List<PotentialMatchEntity> batch, String groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Batch save aborted for groupId={} due to shutdown", groupId);
            throw new IllegalStateException("PotentialMatchStorageProcessor is shutting down");
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        log.debug("Saving batch of {} matches for groupId={}, processingCycleId={}", batch.size(), groupId, processingCycleId);
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute("SET synchronous_commit = OFF");
            conn.createStatement().execute(TEMP_TABLE_SQL);

            CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
            copyManager.copyIn(
                    "COPY temp_potential_matches (id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
                            "FROM STDIN WITH (FORMAT BINARY)",
                    new BinaryCopyInputStream(batch, groupId, domainId, processingCycleId)
            );

            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO public.potential_matches (id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
                            "SELECT id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at " +
                            "FROM temp_potential_matches " +
                            "ON CONFLICT (group_id, reference_id, matched_reference_id) DO UPDATE SET " +
                            "compatibility_score = EXCLUDED.compatibility_score, matched_at = EXCLUDED.matched_at"
            )) {
                stmt.executeUpdate();
            }

            conn.commit();
            long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                    meterRegistry.timer("storage_processor_batch_duration", "groupId", groupId)));
            meterRegistry.counter("storage_processor_batch_processed", "groupId", groupId).increment(batch.size());
            log.debug("Saved batch of {} matches for groupId={}, processingCycleId={} in {} ms",
                    batch.size(), groupId, processingCycleId, durationMs);
        } catch (SQLException | IOException e) {
            meterRegistry.counter("storage_processor_errors", "groupId", groupId, "error_type", "batch_save").increment(batch.size());
            log.error("Batch save failed for groupId={}, processingCycleId={}: {}", groupId, processingCycleId, e.getMessage());
            throw new CompletionException("Batch save failed", e);
        }
    }

    public CompletableFuture<Void> saveAndFinalizeMatches(
            List<PotentialMatchEntity> matches, String groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Save and finalize aborted for groupId={} due to shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("StorageProcessor is shutting down"));
        }
        if (matches.isEmpty()) {
            log.debug("No matches to save for groupId={}, processingCycleId={}", groupId, processingCycleId);
            return CompletableFuture.completedFuture(null);
        }

        List<PotentialMatchEntity> safeMatches = new ArrayList<>(matches);
        Timer.Sample sample = Timer.start(meterRegistry);
        long nodeCount = safeMatches.stream().map(PotentialMatchEntity::getReferenceId).distinct().count();
        log.debug("Queueing save and finalize of {} matches for {} nodes for groupId={}, processingCycleId={}",
                safeMatches.size(), nodeCount, groupId, processingCycleId);

        try {
            storageSemaphore.acquire();
            log.debug("Acquired storageSemaphore for save and finalize, permits left: {}", storageSemaphore.availablePermits());
            return CompletableFuture.runAsync(() -> {
                        try (Connection conn = dataSource.getConnection()) {
                            conn.setAutoCommit(false);
                            conn.createStatement().execute("SET synchronous_commit = OFF");
                            conn.createStatement().execute(TEMP_TABLE_SQL);
                            log.debug("Created temp_potential_matches for groupId={}, processingCycleId={}", groupId, processingCycleId);

                            CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
                            copyManager.copyIn(
                                    "COPY temp_potential_matches (id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
                                            "FROM STDIN WITH (FORMAT BINARY)",
                                    new BinaryCopyInputStream(safeMatches, groupId, domainId, processingCycleId)
                            );
                            meterRegistry.counter("storage_processor_temp_table_saved_total", "groupId", groupId)
                                    .increment(safeMatches.size());

                            try (PreparedStatement stmt = conn.prepareStatement(UPSERT_POTENTIAL_MATCHES_SQL)) {
                                stmt.setString(1, groupId);
                                stmt.setString(2, processingCycleId);
                                int rowsAffected = stmt.executeUpdate();
                                log.debug("Finalized {} matches for {} nodes for groupId={}, processingCycleId={}",
                                        rowsAffected, nodeCount, groupId, processingCycleId);
                                meterRegistry.counter("storage_processor_finalized_total", "groupId", groupId).increment(rowsAffected);
                            }

                            conn.createStatement().execute(DROP_TEMP_TABLE_SQL);
                            log.debug("Dropped temp_potential_matches for groupId={}, processingCycleId={}", groupId, processingCycleId);

                            conn.commit();
                            long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                                    meterRegistry.timer("storage_processor_save_finalize_duration", "groupId", groupId)));
                            log.info("Saved and finalized {} matches for {} nodes for groupId={} in {} ms",
                                    safeMatches.size(), nodeCount, groupId, durationMs);
                        } catch (SQLException | IOException e) {
                            meterRegistry.counter("storage_processor_errors", "groupId", groupId, "error_type", "save_finalize")
                                    .increment(safeMatches.size());
                            log.error("Failed to save and finalize matches for groupId={}, processingCycleId={}: {}",
                                    groupId, processingCycleId, e.getMessage());
                            throw new CompletionException("Save and finalize failed", e);
                        }
                    }, storageExecutor)
                    .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .whenComplete((result, throwable) -> {
                        storageSemaphore.release();
                        log.debug("Released storageSemaphore for save and finalize, permits left: {}", storageSemaphore.availablePermits());
                    });
        } catch (InterruptedException e) {
            log.error("Semaphore acquisition interrupted for groupId={}.", groupId);
            Thread.currentThread().interrupt();
            throw new CompletionException("Semaphore acquisition failed", e);
        }
    }
}