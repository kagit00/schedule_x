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


@Slf4j
@Component
public class PerfectMatchStorageProcessor {
    private static final long SAVE_OPERATION_TIMEOUT_MS = 1_800_000;

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

    @PreDestroy
    private void shutdown() {
        shutdownInitiated = true;
        try {
            ioExecutor.shutdown();
            if (!ioExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                log.warn("I/O executor did not terminate gracefully in 60 seconds. Forcing shutdown.");
                ioExecutor.shutdownNow();
            }
            dataSource.close();
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

    private void saveInBatches(List<PerfectMatchEntity> matches, UUID groupId, UUID domainId, String processingCycleId) {
        List<List<PerfectMatchEntity>> batches = BatchUtils.partition(matches, batchSize);
        for (List<PerfectMatchEntity> batch : batches) {
            try {
                saveBatch(batch, groupId, domainId, processingCycleId);
            } catch (Exception e) {
                log.error("Failed to save a batch for groupId={}, processingCycleId={}: {}",
                        groupId, processingCycleId, e.getMessage(), e);
                throw new CompletionException("One or more batches failed to save", e);
            }
        }
    }

    @Transactional
    @Retryable(
            value = {SQLException.class, TimeoutException.class},
            maxAttempts = 3,
            backoff = @Backoff(
                    delay = 1000,
                    multiplier = 2
            )
    )
    private void saveBatch(List<PerfectMatchEntity> batch, UUID groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated) {
            log.warn("Batch save aborted for groupId={} due to PerfectMatchStorageProcessor shutdown", groupId);
            throw new IllegalStateException("PerfectMatchStorageProcessor is shutting down");
        }

        final Timer.Sample sample = Timer.start(meterRegistry);
        log.debug("Saving batch of {} matches for groupId={}, processingCycleId={}", batch.size(), groupId, processingCycleId);

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute("SET synchronous_commit = OFF");
            conn.createStatement().execute(QueryUtils.getPrefectMatchesTempTableSQL());

            CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
            CopyStreamSerializer<PerfectMatchEntity> serializer =
                    new PerfectMatchSerializer(batch, groupId, domainId, processingCycleId); // Pass processingCycleId

            InputStream binaryStream = new BinaryCopyInputStream<>(batch, serializer);

            copyManager.copyIn(
                    "COPY temp_perfect_matches (id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
                            "FROM STDIN WITH (FORMAT BINARY)",
                    binaryStream
            );

            try (PreparedStatement stmt = conn.prepareStatement(QueryUtils.getUpsertPerfectMatchesSql())) {
                stmt.executeUpdate();
            }

            conn.commit();
            long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                    meterRegistry.timer("perfect_match_storage_batch_duration", "groupId", groupId.toString())));
            meterRegistry.counter("perfect_match_storage_batch_processed_total", "groupId", groupId.toString()).increment(batch.size());
            log.debug("Saved batch of {} matches for groupId={}, processingCycleId={} in {} ms",
                    batch.size(), groupId, processingCycleId, durationMs);

        } catch (SQLException | IOException e) {
            log.error("Batch save failed for groupId={}, processingCycleId={}: {}", groupId, processingCycleId, e.getMessage(), e);
            throw new CompletionException("Batch save failed", e);
        }
    }
}
