package com.shedule.x.processors;

import com.shedule.x.config.factory.BinaryCopyInputStream;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.utils.db.BatchUtils;
import com.shedule.x.utils.db.QueryUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Slf4j
@Component
public class PotentialMatchStorageProcessor {

    private static final int MAX_CONCURRENT_SAVES = 16;
    private static final long SAVE_TIMEOUT_MS = 1_800_000;
    private static final String TEMP_TABLE_COPY_SQL = "COPY temp_potential_matches " +
            "(id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
            "FROM STDIN WITH (FORMAT BINARY)";

    private static final String PG_SESSION_SETUP_SQL =
            "SET LOCAL statement_timeout TO '1500000';" +
                    "SET LOCAL lock_timeout TO '10000';" +
                    "SET LOCAL idle_in_transaction_session_timeout TO '60000';" +
                    "SET LOCAL synchronous_commit = off;";

    private final HikariDataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final ExecutorService storageExecutor;
    private final Semaphore storageSemaphore;
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

    @Value("${matches.save.batch-size:50000}")
    private int batchSize;

    public PotentialMatchStorageProcessor(
            HikariDataSource dataSource,
            MeterRegistry meterRegistry,
            @Qualifier("matchesStorageExecutor") ExecutorService storageExecutor) {

        this.dataSource = dataSource;
        this.meterRegistry = meterRegistry;
        this.storageExecutor = storageExecutor;
        this.storageSemaphore = new Semaphore(MAX_CONCURRENT_SAVES);
    }

    @PreDestroy
    private void shutdown() {
        shutdownInitiated.set(true);
        storageExecutor.shutdown();

        try {
            if (!storageExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
                storageExecutor.shutdownNow();
            }

            dataSource.close();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }


    @Retryable(
            value = SQLException.class,
            exceptionExpression = "@sqlErrorCode.isDeadlock(#root)",
            maxAttempts = 3,
            backoff = @Backoff(delay = 500, multiplier = 2, random = true)
    )
    public CompletableFuture<Void> deleteByGroupId(UUID groupId) {
        if (shutdownInitiated.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shutting down"));
        }

        return CompletableFuture.runAsync(() -> {
            withAdvisoryLock(groupId, conn -> {
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM public.potential_matches WHERE group_id = ?")) {
                    ps.setObject(1, groupId);
                    int rows = ps.executeUpdate();
                    meterRegistry.counter("storage_processor_matches_deleted_total", "groupId", groupId.toString())
                            .increment(rows);
                    log.info("Deleted {} rows for groupId={}", rows, groupId);
                } catch (SQLException ex) {
                    throw new CompletionException(ex);
                }
                return null;
            });
        }, storageExecutor);
    }

    public long countFinalMatches(String groupId, UUID domainId, String processingCycleId) {
        if (shutdownInitiated.get()) {
            throw new IllegalStateException("Shutting down");
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM public.potential_matches " +
                             "WHERE group_id = ? AND domain_id = ? AND processing_cycle_id = ?")) {

            stmt.setString(1, groupId);
            stmt.setObject(2, domainId);
            stmt.setString(3, processingCycleId);
            try (var rs = stmt.executeQuery()) {
                long count = rs.next() ? rs.getLong(1) : 0;
                long ms = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                        meterRegistry.timer("storage_processor_count_duration", "groupId", groupId)));
                log.debug("Counted {} final matches for groupId={} in {} ms", count, groupId, ms);
                return count;
            }
        } catch (SQLException e) {
            meterRegistry.counter("storage_processor_errors", "groupId", groupId, "error_type", "count").increment();
            log.error("Count failed for groupId={}", groupId, e);
            throw new CompletionException(e);
        }
    }

    private void saveAllBatchesInOneTransaction(List<PotentialMatchEntity> all, UUID groupId, UUID domainId, String cycleId) {
        withAdvisoryLock(groupId, conn -> {
            try {
                conn.setAutoCommit(false);
                try (Statement setupStmt = conn.createStatement()) {
                    setupStmt.execute(PG_SESSION_SETUP_SQL);
                    setupStmt.execute(QueryUtils.getPotentialMatchesTempTableSql());
                }

                CopyManager copyMgr = new CopyManager(conn.unwrap(BaseConnection.class));
                List<List<PotentialMatchEntity>> parts = BatchUtils.partition(all, batchSize);

                for (int i = 0; i < parts.size(); i++) {
                    List<PotentialMatchEntity> part = parts.get(i);
                    log.debug("Copying batch {}/{} for groupId={}", i + 1, parts.size(), groupId);
                    copyBatchWithCancellation(copyMgr, part, groupId, domainId, cycleId);
                }

                log.info("All batches copied to temp table for groupId={}. Merging...", groupId);
                try (PreparedStatement mergeStmt = conn.prepareStatement(QueryUtils.getMergePotentialMatchesSql())) {
                    mergeStmt.setObject(1, groupId);
                    mergeStmt.setObject(2, groupId);
                    mergeStmt.setString(3, cycleId);
                    int updatedRows = mergeStmt.executeUpdate();
                    log.info("Merge complete for groupId={}. {} rows affected.", groupId, updatedRows);
                }

                conn.commit();

            } catch (Exception e) {
                try {
                    log.warn("Rolling back transaction for groupId={} due to error: {}", groupId, e.getMessage());
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    log.error("CRITICAL: Failed to rollback transaction for groupId={}", groupId, rollbackEx);
                }
                throw new CompletionException(e);
            }
            return null;
        });
    }

    private void copyBatchWithCancellation(CopyManager copyManager, List<PotentialMatchEntity> batch, UUID groupId, UUID domainId, String cycleId)
            throws SQLException, IOException {

        CopyIn copyIn = null;
        try (InputStream in = new BinaryCopyInputStream<>(batch, new PotentialMatchSerializer(batch, groupId, domainId, cycleId))) {
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

    private CompletableFuture<Void> acquireSemaphoreAsync(
            Semaphore semaphore, UUID groupId) {

        CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                semaphore.acquire();
                future.complete(null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    public CompletableFuture<Void> saveAndFinalizeMatches(
            List<PotentialMatchEntity> matches,
            UUID groupId, UUID domainId, String cycleId) {

        if (shutdownInitiated.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shutting down"));
        }
        if (matches.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        final long ACQUIRE_TIMEOUT_MS = 600_000;

        return acquireSemaphoreAsync(storageSemaphore, groupId)
                .thenComposeAsync(v ->
                                CompletableFuture.runAsync(() -> {
                                            withAdvisoryLock(groupId, conn -> {
                                                try {
                                                    conn.setAutoCommit(false);

                                                    try (Statement st = conn.createStatement()) {
                                                        st.execute(PG_SESSION_SETUP_SQL);
                                                        st.execute(QueryUtils.getPotentialMatchesTempTableSql());
                                                    }

                                                    CopyManager mgr = new CopyManager(conn.unwrap(BaseConnection.class));
                                                    copyBatchWithCancellation(mgr, matches, groupId, domainId, cycleId);

                                                    try (PreparedStatement merge = conn.prepareStatement(QueryUtils.getMergePotentialMatchesSql())) {
                                                        merge.setObject(1, groupId);
                                                        merge.setObject(2, groupId);
                                                        merge.setString(3, cycleId);
                                                        merge.executeUpdate();
                                                    }

                                                    conn.commit();

                                                } catch (Exception e) {
                                                    try { conn.rollback(); } catch (SQLException ignored) {}
                                                    throw new CompletionException(e);
                                                }

                                                return null;
                                            });
                                        }, storageExecutor)
                                        .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                        storageExecutor)
                .whenComplete((v, t) -> {
                    storageSemaphore.release();
                    long ms = Duration.ofNanos(sample.stop(
                            meterRegistry.timer("storage_processor_finalize_duration", "groupId", groupId.toString())
                    )).toMillis();

                    if (t == null) {
                        log.info("Finalized {} matches for groupId={} in {} ms", matches.size(), groupId, ms);
                    } else {
                        log.error("Finalize failed for groupId={}: {}", groupId, t.toString(), t);
                    }
                });
    }

    public CompletableFuture<Void> savePotentialMatches(
            List<PotentialMatchEntity> matches,
            UUID groupId, UUID domainId, String processingCycleId) {

        if (shutdownInitiated.get() || matches.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final long ACQUIRE_TIMEOUT_MS = 600_000;
        List<PotentialMatchEntity> safeMatches = List.copyOf(matches);
        Timer.Sample sample = Timer.start(meterRegistry);

        return acquireSemaphoreAsync(storageSemaphore, groupId)
                .thenComposeAsync(v ->
                                CompletableFuture.runAsync(
                                                () -> saveAllBatchesInOneTransaction(safeMatches, groupId, domainId, processingCycleId),
                                                storageExecutor)
                                        .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                        storageExecutor)
                .whenComplete((v, t) -> {
                    storageSemaphore.release();
                    long ms = Duration.ofNanos(sample.stop(
                            meterRegistry.timer("storage_processor_duration", "groupId", groupId.toString())
                    )).toMillis();

                    if (t == null) {
                        meterRegistry.counter("storage_processor_matches_saved_total", "groupId", groupId.toString())
                                .increment(safeMatches.size());
                        log.info("SUCCESS: Saved {} potential matches for groupId={} in {} ms.",
                                safeMatches.size(), groupId, ms);
                    } else {
                        Throwable cause = (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
                        meterRegistry.counter("storage_processor_errors", "groupId", groupId.toString(), "error_type", "save")
                                .increment(safeMatches.size());
                        log.error("FAILED: Could not save matches for groupId={}. Total time: {} ms. Reason: {}",
                                groupId, ms, cause.toString(), cause);
                    }
                });
    }

}