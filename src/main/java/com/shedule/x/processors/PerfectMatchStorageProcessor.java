package com.shedule.x.processors;


import com.shedule.x.config.factory.BinaryCopyInputStream;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.utils.db.BatchUtils;
import com.shedule.x.utils.db.QueryUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;


@Slf4j
@Component
public class PerfectMatchStorageProcessor {

    private static final int MAX_CONCURRENT_SAVES = 16;
    private static final long SAVE_TIMEOUT_MS = 1_800_000;
    private static final String TEMP_TABLE_COPY_SQL = "COPY temp_perfect_matches " +
            "(id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
            "FROM STDIN WITH (FORMAT BINARY)";

    private static final String PG_SESSION_SETUP_SQL =
            "SET LOCAL statement_timeout TO '1500000';" +
                    "SET LOCAL lock_timeout TO '10000';" +
                    "SET LOCAL idle_in_transaction_session_timeout TO '60000';" +
                    "SET LOCAL synchronous_commit = off;";

    private final HikariDataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final ExecutorService ioExecutor;
    private final Semaphore storageSemaphore;
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

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
        this.storageSemaphore = new Semaphore(MAX_CONCURRENT_SAVES);
        meterRegistry.gauge("system_cpu_usage", ManagementFactory.getOperatingSystemMXBean(), OperatingSystemMXBean::getSystemLoadAverage);
    }

    @PreDestroy
    private void shutdown() {
        shutdownInitiated.set(true);
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

    @Retryable(
            value = SQLException.class,
            exceptionExpression = "@sqlErrorCode.isDeadlock(#root)",
            maxAttempts = 3,
            backoff = @Backoff(delay = 500, multiplier = 2, random = true)
    )
    public CompletableFuture<Void> savePerfectMatches(List<PerfectMatchEntity> matches, UUID groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated.get()) {
            log.warn("Save aborted for groupId={} due to PerfectMatchStorageProcessor shutdown", groupId);
            return CompletableFuture.failedFuture(new IllegalStateException("PerfectMatchStorageProcessor is shutting down"));
        }

        if (matches.isEmpty()) {
            log.debug("No matches to save for groupId={}", groupId);
            return CompletableFuture.completedFuture(null);
        }

        final List<PerfectMatchEntity> safeMatches = List.copyOf(matches);
        final Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Queueing save of {} Perfect matches for groupId={}, domainId={}, processingCycleId={}",
                safeMatches.size(), groupId, domainId, processingCycleId);

        return acquireSemaphoreAsync(storageSemaphore, groupId)
                .thenComposeAsync(v ->
                                CompletableFuture.runAsync(() -> {
                                            withAdvisoryLock(groupId, conn -> {
                                                saveAllBatchesInOneTransaction(safeMatches, groupId, domainId, processingCycleId, conn);
                                                return null;
                                            });
                                        }, ioExecutor)
                                        .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                        ioExecutor)
                .whenComplete((result, throwable) -> {
                    storageSemaphore.release();
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                            meterRegistry.timer("perfect_match_storage_duration", "groupId", groupId.toString())));

                    if (throwable != null) {
                        meterRegistry.counter("perfect_match_storage_errors_total",
                                        "groupId", groupId.toString(), "error_type", throwable.getClass().getSimpleName())
                                .increment(safeMatches.size());
                        log.error("Failed to save {} matches for groupId={}, processingCycleId={}: {}",
                                safeMatches.size(), groupId, processingCycleId, throwable.toString(), throwable);
                    } else {
                        meterRegistry.counter("perfect_match_storage_matches_saved_total", "groupId", groupId.toString())
                                .increment(safeMatches.size());
                        log.info("Successfully saved {} PerfectMatchEntity matches for groupId={} in {} ms",
                                safeMatches.size(), groupId, durationMs);
                    }
                });
    }

    private void saveAllBatchesInOneTransaction(List<PerfectMatchEntity> matches, UUID groupId, UUID domainId, String processingCycleId, Connection conn) {
        try {
            conn.setAutoCommit(false);

            try (Statement setupStmt = conn.createStatement()) {
                setupStmt.execute(PG_SESSION_SETUP_SQL);
                setupStmt.execute(QueryUtils.getPrefectMatchesTempTableSQL());
            }

            List<List<PerfectMatchEntity>> batches = BatchUtils.partition(matches, batchSize);
            CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));

            for (int i = 0; i < batches.size(); i++) {
                List<PerfectMatchEntity> batch = batches.get(i);
                log.debug("Copying batch {}/{} for groupId={}", i + 1, batches.size(), groupId);
                copyBatchWithCancellation(copyManager, batch, groupId, domainId, processingCycleId);
            }

            log.info("All batches copied to temp table for groupId={}. Merging...", groupId);
            try (PreparedStatement stmt = conn.prepareStatement(QueryUtils.getUpsertPerfectMatchesSql())) {
                int updated = stmt.executeUpdate();
                log.info("Upsert complete for groupId={}. {} rows affected.", groupId, updated);
            }

            conn.commit();

        } catch (Exception e) {
            try {
                log.warn("Rolling back transaction for groupId={} due to error: {}", groupId, e.getMessage());
                conn.rollback();
            } catch (SQLException rollbackEx) {
                log.error("CRITICAL: Failed to rollback transaction for groupId={}", groupId, rollbackEx);
            }
            throw new CompletionException("One or more batches failed to save", e);
        }
    }

    private void copyBatchWithCancellation(CopyManager copyManager, List<PerfectMatchEntity> batch, UUID groupId, UUID domainId, String processingCycleId)
            throws SQLException, IOException {

        CopyIn copyIn = null;
        try (InputStream in = new BinaryCopyInputStream<>(batch, new PerfectMatchSerializer(batch, groupId, domainId, processingCycleId))) {
            copyIn = copyManager.copyIn(TEMP_TABLE_COPY_SQL);

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                copyIn.writeToCopy(buffer, 0, bytesRead);
            }
            copyIn.endCopy();

        } catch (Exception e) {
            if (copyIn != null && copyIn.isActive()) {
                try {
                    log.warn("Error during COPY operation for groupId={}. Attempting to cancel...", groupId);
                    copyIn.cancelCopy();
                    log.info("COPY operation cancelled successfully for groupId={}.", groupId);
                } catch (SQLException cancelEx) {
                    log.error("CRITICAL: Failed to cancel COPY operation for groupId={}. Connection may be left in an inconsistent state.", groupId, cancelEx);
                }
            }
            throw e;
        }
    }

    private <T> void withAdvisoryLock(UUID groupId, Function<Connection, T> action) {
        try (Connection conn = dataSource.getConnection()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CompletionException(new InterruptedException("Thread was interrupted before acquiring lock for group " + groupId));
            }

            try (PreparedStatement lockStmt = conn.prepareStatement(QueryUtils.getAcquireGroupLockSql())) {
                lockStmt.setString(1, groupId.toString());
                lockStmt.execute();
                action.apply(conn);
            } finally {
                try (Statement st = conn.createStatement()) {
                    st.execute("RESET ALL;");
                } catch (SQLException resetEx) {
                    log.error("CRITICAL: Failed to execute 'RESET ALL' for connection used by groupId={}. Connection state may be dirty.", groupId, resetEx);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to acquire advisory lock or execute database action for groupId={}", groupId, e);
            throw new CompletionException(e);
        }
    }

    private CompletableFuture<Void> acquireSemaphoreAsync(Semaphore semaphore, UUID groupId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                semaphore.acquire();
                future.complete(null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            }
        }, ioExecutor);
        return future;
    }
}