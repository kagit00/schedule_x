package com.shedule.x.processors;

import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import com.shedule.x.utils.db.BatchUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class PerfectMatchStorageProcessor {
    private static final String COPY_TEMP_SQL =
            "CREATE TEMP TABLE temp_perfect_matches (\n" +
                    "    id UUID NOT NULL,\n" +
                    "    group_id VARCHAR(50),\n" +
                    "    domain_id UUID,\n" +
                    "    reference_id VARCHAR(50),\n" +
                    "    matched_reference_id VARCHAR(50),\n" +
                    "    compatibility_score DOUBLE PRECISION,\n" +
                    "    matched_at TIMESTAMP\n" +
                    ") ON COMMIT DROP";

    private static final String UPSERT_PERFECT_MATCHES_SQL =
            "INSERT INTO public.perfect_matches (\n" +
                    "    id, group_id, domain_id, reference_id, matched_reference_id, compatibility_score, matched_at\n" +
                    ") SELECT id, group_id, domain_id, reference_id, matched_reference_id, compatibility_score, matched_at\n" +
                    "FROM temp_perfect_matches\n" +
                    "ON CONFLICT (group_id, reference_id, matched_reference_id)\n" +
                    "DO UPDATE SET\n" +
                    "    compatibility_score = EXCLUDED.compatibility_score,\n" +
                    "    matched_at = EXCLUDED.matched_at";

    private static final long SAVE_TIMEOUT_MS = 1_800_000;

    private final HikariDataSource dataSource;
    private final MeterRegistry meterRegistry;
    private final Executor storageExecutor;

    @Value("${import.batch-size:1000}") // Reduced from 50000
    private int batchSize;

    public PerfectMatchStorageProcessor(
            HikariDataSource dataSource,
            MeterRegistry meterRegistry,
            @Qualifier("matchesStorageExecutor") Executor storageExecutor
    ) {
        this.dataSource = dataSource;
        this.meterRegistry = meterRegistry;
        this.storageExecutor = storageExecutor;

        // Add CPU usage metric
        meterRegistry.gauge("system_cpu_usage", ManagementFactory.getOperatingSystemMXBean(), OperatingSystemMXBean::getSystemLoadAverage);
    }

    public CompletableFuture<Void> savePerfectMatches(List<PerfectMatchEntity> matches, String groupId, UUID domainId) {
        if (matches.isEmpty()) {
            log.debug("No perfect matches to save for groupId={}", groupId);
            return CompletableFuture.completedFuture(null);
        }

        List<PerfectMatchEntity> safeMatches = new ArrayList<>(matches);
        Timer.Sample sample = Timer.start(meterRegistry);
        log.info("Queueing save of {} perfect matches for groupId={}, domainId={}", safeMatches.size(), groupId, domainId);
        log.info("StorageExecutor queue: {}, active: {}, Hikari active: {}",
                ((ThreadPoolTaskExecutor) storageExecutor).getThreadPoolExecutor().getQueue().size(),
                ((ThreadPoolTaskExecutor) storageExecutor).getThreadPoolExecutor().getActiveCount(),
                dataSource.getHikariPoolMXBean().getActiveConnections());

        return CompletableFuture.runAsync(() -> saveInBatches(safeMatches, groupId, domainId), storageExecutor)
                .orTimeout(SAVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .whenComplete((result, throwable) -> {
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                            meterRegistry.timer("perfect_match_import_duration", "groupId", groupId)));
                    if (throwable != null) {
                        meterRegistry.counter("perfect_match_import_errors", "groupId", groupId)
                                .increment(safeMatches.size());
                        log.error("Failed to save {} perfect matches for groupId={}: {}", safeMatches.size(), groupId,
                                throwable.getMessage(), throwable);
                    } else {
                        meterRegistry.counter("perfect_match_import_total", "groupId", groupId)
                                .increment(safeMatches.size());
                        log.info("Saved {} perfect matches for groupId={} in {} ms", safeMatches.size(), groupId, durationMs);
                    }
                });
    }

    private void saveInBatches(List<PerfectMatchEntity> matches, String groupId, UUID domainId) {
        List<List<PerfectMatchEntity>> batches = BatchUtils.partition(matches, batchSize);
        for (List<PerfectMatchEntity> batch : batches) {
            saveBatch(batch, groupId, domainId);
        }
    }

    @Transactional(timeout = 60)
    @Retryable(
            value = { InternalServerErrorException.class, SQLException.class, TimeoutException.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    private void saveBatch(List<PerfectMatchEntity> batch, String groupId, UUID domainId) {
        Timer.Sample sample = Timer.start(meterRegistry);
        log.debug("Saving batch of {} perfect matches for groupId={}", batch.size(), groupId);
        try (Connection conn = dataSource.getConnection();
             InputStream inputStream = toPipeStream(batch, groupId, domainId)) {

            conn.setAutoCommit(false);
            createTempTable(conn);

            CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
            copyManager.copyIn(
                    "COPY temp_perfect_matches (id, group_id, domain_id, reference_id, matched_reference_id, compatibility_score, matched_at) " +
                            "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')",
                    inputStream
            );

            executeUpsert(conn);
            conn.commit();
            long durationMs = TimeUnit.NANOSECONDS.toMillis(sample.stop(
                    meterRegistry.timer("perfect_match_import_batch_duration", "groupId", groupId)));
            meterRegistry.counter("perfect_match_import_batch_processed", "groupId", groupId).increment(batch.size());
            if (durationMs > 10_000) {
                log.warn("Slow batch save of {} perfect matches for groupId={} took {} ms", batch.size(), groupId, durationMs);
            }
            log.info("Saved batch of {} perfect matches for groupId={} in {} ms", batch.size(), groupId, durationMs);
        } catch (SQLException | IOException e) {
            meterRegistry.counter("perfect_match_import_errors", "groupId", groupId).increment(batch.size());
            log.error("Batch save failed for groupId={}: {}", groupId, e.getMessage(), e);
            throw new InternalServerErrorException("Batch save failed: " + e.getMessage());
        }
    }

    private void createTempTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(COPY_TEMP_SQL);
            log.debug("Created temp table temp_perfect_matches");
        }
    }

    private void executeUpsert(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(UPSERT_PERFECT_MATCHES_SQL);
            log.debug("Executed UPSERT for temp_perfect_matches");
        }
    }

    private InputStream toPipeStream(List<PerfectMatchEntity> batch, String groupId, UUID domainId) {
        PipedOutputStream out = new PipedOutputStream();
        PipedInputStream in;
        try {
            in = new PipedInputStream(out, 64 * 1024);
        } catch (IOException e) {
            throw new InternalServerErrorException("Failed to initialize piped streams: " + e.getMessage());
        }

        CompletableFuture<Void> writerFuture = CompletableFuture.runAsync(() -> {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
                for (PerfectMatchEntity match : batch) {
                    writer.write(UUID.randomUUID() + "\t" +
                            groupId + "\t" +
                            domainId + "\t" +
                            match.getReferenceId() + "\t" +
                            match.getMatchedReferenceId() + "\t" +
                            match.getCompatibilityScore() + "\t" +
                            DefaultValuesPopulator.getCurrentTimestamp().toString().replace(" ", "T") + "\n");
                }
            } catch (IOException e) {
                log.error("Error writing to piped stream for groupId={}: {}", groupId, e.getMessage(), e);
                throw new UncheckedIOException(e);
            }
        }, storageExecutor);

        writerFuture.exceptionally(ex -> {
            log.error("Background stream writer failed for groupId={}", groupId, ex);
            return null;
        });

        return in;
    }
}