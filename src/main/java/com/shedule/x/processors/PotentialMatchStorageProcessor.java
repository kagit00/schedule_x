package com.shedule.x.processors;

import com.shedule.x.config.factory.BinaryCopyInputStream;
import com.shedule.x.config.factory.CopyStreamSerializer;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.utils.db.BatchUtils;
import com.shedule.x.utils.db.QueryUtils;
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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Slf4j
@Component
public class PotentialMatchStorageProcessor {

    private static final long SAVE_TIMEOUT_MS = 300_000;
    private static final int  MAX_CONCURRENT_SAVES = 12;

    private final HikariDataSource dataSource;
    private final MeterRegistry   meterRegistry;
    private final ExecutorService storageExecutor;
    private final Semaphore       storageSemaphore;
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);

    @Value("${matches.save.batch-size:5000}")
    private int batchSize;

    public PotentialMatchStorageProcessor(
            HikariDataSource dataSource,
            MeterRegistry meterRegistry,
            @Qualifier("matchesStorageExecutor") ExecutorService storageExecutor) {

        this.dataSource        = dataSource;
        this.meterRegistry     = meterRegistry;
        this.storageExecutor   = storageExecutor;
        this.storageSemaphore  = new Semaphore(MAX_CONCURRENT_SAVES);
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

    public CompletableFuture<Void> savePotentialMatches(
            List<PotentialMatchEntity> matches,
            UUID groupId, UUID domainId, String processingCycleId) {

        if (shutdownInitiated.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shutting down"));
        }
        if (matches.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        List<PotentialMatchEntity> safe = List.copyOf(matches);
        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            if (!storageSemaphore.tryAcquire(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                return CompletableFuture.failedFuture(new TimeoutException("Storage semaphore timeout"));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(ie);
        }

        return CompletableFuture.runAsync(
                        () -> saveInBatches(safe, groupId, domainId, processingCycleId),
                        storageExecutor)
                .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .whenComplete((v, t) -> {
                    storageSemaphore.release();
                    long ms = Duration.ofNanos(sample.stop(
                            meterRegistry.timer("storage_processor_duration", "groupId", groupId.toString())
                    )).toMillis();

                    if (t == null) {
                        meterRegistry.counter("storage_processor_matches_saved_total", "groupId", groupId.toString())
                                .increment(safe.size());
                        log.info("Saved {} potential matches for groupId={} in {} ms", safe.size(), groupId, ms);
                    } else {
                        meterRegistry.counter("storage_processor_errors", "groupId", groupId.toString(), "error_type", "save")
                                .increment(safe.size());
                        log.error("Failed to save matches for groupId={}: {}", groupId, t.toString(), t);
                    }
                });
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

        try {
            if (!storageSemaphore.tryAcquire(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                return CompletableFuture.failedFuture(new TimeoutException("Storage semaphore timeout"));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(ie);
        }

        return CompletableFuture.runAsync(() -> {
                    withAdvisoryLock(groupId, () -> {
                        saveSingleBatch(matches, groupId, domainId, cycleId);
                        return null;
                    });
                }, storageExecutor)
                .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
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
            withAdvisoryLock(groupId, () -> {
                try (Connection c = dataSource.getConnection();
                     PreparedStatement ps = c.prepareStatement(
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


    private void saveInBatches(List<PotentialMatchEntity> all, UUID groupId, UUID domainId, String cycleId) {
        List<List<PotentialMatchEntity>> parts = BatchUtils.partition(all, batchSize);
        for (List<PotentialMatchEntity> part : parts) {
            withAdvisoryLock(groupId, () -> {
                saveSingleBatch(part, groupId, domainId, cycleId);
                return null;
            });
        }
    }

    @Transactional
    @Retryable(
            value = SQLException.class,
            exceptionExpression = "@sqlErrorCode.isDeadlock(#root)",
            maxAttempts = 3,
            backoff = @Backoff(delay = 500, multiplier = 2, random = true)
    )
    private void saveSingleBatch(List<PotentialMatchEntity> batch, UUID groupId, UUID domainId, String cycleId) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute("SET synchronous_commit = OFF");
            conn.createStatement().execute(QueryUtils.getPotentialMatchesTempTableSql());

            // COPY IN
            CopyManager copyMgr = new CopyManager(conn.unwrap(BaseConnection.class));
            CopyStreamSerializer<PotentialMatchEntity> ser = new PotentialMatchSerializer(batch, groupId, domainId, cycleId);
            try (InputStream in = new BinaryCopyInputStream<>(batch, ser)) {
                copyMgr.copyIn(
                        "COPY temp_potential_matches " +
                                "(id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
                                "FROM STDIN WITH (FORMAT BINARY)", in);
            }

            try (PreparedStatement merge = conn.prepareStatement(QueryUtils.getMergePotentialMatchesSql())) {
                merge.setObject(1, groupId);
                merge.setObject(2, groupId);
                merge.setString(3, cycleId);
                merge.executeUpdate();
            }

            conn.commit();
        } catch (SQLException | IOException ex) {
            throw new CompletionException(ex);
        }
    }


    private <T> void withAdvisoryLock(UUID groupId, Supplier<T> action) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement lockStmt = conn.prepareStatement(QueryUtils.getAcquireGroupLockSql())) {

            lockStmt.setString(1, groupId.toString());
            lockStmt.execute();

            action.get();
        } catch (SQLException e) {
            log.error("Failed to acquire advisory lock for groupId={}", groupId, e);
            throw new CompletionException(e);
        }
    }

    private void withAdvisoryLock(UUID groupId, Runnable action) {
        withAdvisoryLock(groupId, () -> { action.run(); return null; });
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
}