package com.shedule.x.processors;


import com.shedule.x.config.factory.BinaryCopyInputStream;
import com.shedule.x.config.factory.CopyStreamSerializer;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.utils.db.BatchUtils;
import com.shedule.x.utils.db.QueryUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
@Component
public class PerfectMatchStorageProcessor {
    // Shorter timeout to fail faster if the database is unresponsive,
    // contributing to lower *perceived* latency on failure.
    private static final long SAVE_OPERATION_TIMEOUT_MS = 60_000;

    private final HikariDataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final ExecutorService ioExecutor;
    private volatile boolean shutdownInitiated = false;

    @Value("${import.batch-size:1000}")
    private int batchSize;

    public PerfectMatchStorageProcessor(
            HikariDataSource dataSource,
            MeterRegistry meterRegistry,
            @Qualifier("ioExecutorService") ExecutorService ioExecutor
    ) {
        this.dataSource = dataSource;
        this.meterRegistry = meterRegistry;
        this.ioExecutor = ioExecutor;
        meterRegistry.gauge("system_cpu_usage", ManagementFactory.getOperatingSystemMXBean(), OperatingSystemMXBean::getSystemLoadAverage);
    }

    // --- DEADLOCK FIX & SHUTDOWN OPTIMIZATION ---
    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;

        // CRITICAL FIX: Close the data source NOW to unblock any threads
        // in ioExecutor that might be waiting for a connection.
        dataSource.close();
        ioExecutor.shutdown();

        try {
            if (!ioExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                log.warn("I/O executor did not terminate gracefully in 60 seconds. Forcing shutdown.");
                ioExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted while awaiting I/O executor termination.", e);
            Thread.currentThread().interrupt();
        }
    }

    public CompletableFuture<Void> savePerfectMatches(List<PerfectMatchEntity> matches, UUID groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Save aborted for groupId={} due to PerfectMatchStorageProcessor shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("PerfectMatchStorageProcessor is shutting down"));
        }

        if (matches.isEmpty()) {
            log.debug("No matches to save for groupId={}", groupId);
            return CompletableFuture.completedFuture(null);
        }

        final List<PerfectMatchEntity> safeMatches = new ArrayList<>(matches);
        final Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Queueing save of {} Perfect matches for groupId={}, domainId={}, processingCycleId={}",
                safeMatches.size(), groupId, domainId, processingCycleId);

        return CompletableFuture.runAsync(() -> {
                    // Retries (if necessary) should ideally wrap this call
                    saveInBatches(safeMatches, groupId, domainId, processingCycleId);
                }, ioExecutor)
                .orTimeout(SAVE_OPERATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .whenComplete((result, throwable) -> {
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                            meterRegistry.timer("perfect_match_storage_duration", "groupId", groupId.toString())));
                    if (throwable != null) {
                        meterRegistry.counter("perfect_match_storage_errors_total",
                                        "groupId", groupId.toString(), "error_type", throwable.getClass().getSimpleName())
                                .increment(safeMatches.size());
                        log.error("Failed to save {} matches for groupId={}, processingCycleId={}: {}",
                                safeMatches.size(), groupId, processingCycleId, throwable.getMessage(), throwable);
                    } else {
                        meterRegistry.counter("perfect_match_storage_matches_saved_total", "groupId", groupId.toString())
                                .increment(safeMatches.size());
                        log.info("Successfully saved {} PerfectMatchEntity matches for groupId={} in {} ms",
                                safeMatches.size(), groupId, durationMs);
                    }
                });
    }

    // --- ULTRA-LOW LATENCY OPTIMIZATION: Amortize Connection and Transaction Overhead ---
    private void saveInBatches(List<PerfectMatchEntity> matches, UUID groupId, UUID domainId, String processingCycleId) {
        // Use a single connection/transaction for the entire list of matches to minimize overhead.
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // 1. Database-Level Optimization for Low Latency Commit
            conn.createStatement().execute("SET synchronous_commit = OFF");
            // 2. Amortize Setup Cost: Create temp table ONCE per request
            conn.createStatement().execute(QueryUtils.getPrefectMatchesTempTableSQL());

            List<List<PerfectMatchEntity>> batches = BatchUtils.partition(matches, batchSize);

            for (List<PerfectMatchEntity> batch : batches) {
                // Call the internal save function, passing the single connection
                saveBatchInternal(conn, batch, groupId, domainId, processingCycleId);
            }

            // 3. Low Latency Commit: Commit ONCE after all batches are done
            conn.commit();

        } catch (Exception e) {
            log.error("Failed to save all batches for groupId={}, processingCycleId={}", groupId, processingCycleId, e);
            // Throw CompletionException to be caught by the CompletableFuture's error handler
            throw new CompletionException("One or more batches failed to save", e);
        }
    }

    /**
     * Internal function to save a batch using an existing connection.
     * No @Transactional or external connection acquisition/commit logic.
     */
    private void saveBatchInternal(Connection conn, List<PerfectMatchEntity> batch, UUID groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Batch save aborted for groupId={} due to PerfectMatchStorageProcessor shutdown", groupId);
            throw new IllegalStateException("PerfectMatchStorageProcessor is shutting down");
        }

        final Timer.Sample sample = Timer.start(meterRegistry);
        log.debug("Saving batch of {} matches for groupId={}, processingCycleId={}", batch.size(), groupId, processingCycleId);

        try {
            CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
            CopyStreamSerializer<PerfectMatchEntity> serializer =
                    new PerfectMatchSerializer(batch, groupId, domainId, processingCycleId);

            InputStream binaryStream = new BinaryCopyInputStream<>(batch, serializer);

            // Fast COPY operation
            copyManager.copyIn(
                    "COPY temp_perfect_matches (id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
                            "FROM STDIN WITH (FORMAT BINARY)",
                    binaryStream
            );

            // Execute the UPSERT from the temp table to the final table
            try (PreparedStatement stmt = conn.prepareStatement(QueryUtils.getUpsertPerfectMatchesSql())) {
                stmt.executeUpdate();
            }

            // Metrics
            long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                    meterRegistry.timer("perfect_match_storage_batch_duration", "groupId", groupId.toString())));
            meterRegistry.counter("perfect_match_storage_batch_processed_total", "groupId", groupId.toString()).increment(batch.size());
            log.debug("Saved batch of {} matches for groupId={}, processingCycleId={} in {} ms",
                    batch.size(), groupId, processingCycleId, durationMs);

        } catch (SQLException | IOException e) {
            log.error("Batch save failed for groupId={}, processingCycleId={}: {}", groupId, processingCycleId, e.getMessage(), e);
            // Throw a RuntimeException/wrapped exception to trigger the transaction rollback in the calling function
            throw new RuntimeException("Batch save failed", e);
        }
    }
}