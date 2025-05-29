package com.shedule.x.processors;

import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.utils.basic.Constant;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class NodeMetadataBatchWriter {
    private static final int METADATA_BATCH_SIZE = 30000;
    private static final String COPY_TEMP_SQL =
            "CREATE TEMP TABLE temp_node_metadata (node_id UUID, meta_key TEXT, meta_value TEXT) ON COMMIT DROP";

    private static final String UPSERT_METADATA_SQL =
            "INSERT INTO public.node_metadata (node_id, meta_key, meta_value)\n" +
                    "SELECT node_id, meta_key, meta_value FROM temp_node_metadata\n" +
                    "ON CONFLICT (node_id, meta_key)\n" +
                    "DO UPDATE SET meta_value = EXCLUDED.meta_value";


    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;
    private final RetryTemplate retryTemplate;
    private final TransactionTemplate transactionTemplate;

    public void batchInsertMetadata(Map<UUID, Map<String, String>> metadataByNode) {
        if (metadataByNode.isEmpty()) {
            log.debug("No metadata to insert. Skipping.");
            return;
        }

        long startTime = System.nanoTime();
        ConcurrentHashMap<UUID, Boolean> failedNodeIds = new ConcurrentHashMap<>();
        List<List<Map.Entry<UUID, Map.Entry<String, String>>>> batches = partitionMetadata(metadataByNode);

        for (List<Map.Entry<UUID, Map.Entry<String, String>>> batch : batches) {
            insertBatch(batch, failedNodeIds);
        }

        recordMetrics(metadataByNode.size(), failedNodeIds.size(), startTime);

        if (!failedNodeIds.isEmpty()) {
            log.error("Failed to insert metadata for {} node_ids: {}", failedNodeIds.size(), failedNodeIds.keySet());
            throw new InternalServerErrorException("Metadata insert failed for " + failedNodeIds.size() + " nodes");
        }
    }

    private List<List<Map.Entry<UUID, Map.Entry<String, String>>>> partitionMetadata(Map<UUID, Map<String, String>> metadataByNode) {
        List<List<Map.Entry<UUID, Map.Entry<String, String>>>> result = new ArrayList<>();
        List<Map.Entry<UUID, Map.Entry<String, String>>> currentBatch = new ArrayList<>();
        int currentSize = 0;

        for (Map.Entry<UUID, Map<String, String>> nodeEntry : metadataByNode.entrySet()) {
            List<Map.Entry<UUID, Map.Entry<String, String>>> nodeEntries = nodeEntry.getValue().entrySet().stream()
                    .map(meta -> Map.entry(nodeEntry.getKey(), meta))
                    .collect(Collectors.toList());

            if (currentSize + nodeEntries.size() > METADATA_BATCH_SIZE && !currentBatch.isEmpty()) {
                result.add(currentBatch);
                currentBatch = new ArrayList<>();
                currentSize = 0;
            }

            currentBatch.addAll(nodeEntries);
            currentSize += nodeEntries.size();
        }

        if (!currentBatch.isEmpty()) {
            result.add(currentBatch);
        }

        return result;
    }

    private void insertBatch(List<Map.Entry<UUID, Map.Entry<String, String>>> batch, ConcurrentHashMap<UUID, Boolean> failedNodeIds) {
        retryTemplate.execute(context -> {
            String csvData = buildCsvRows(batch);
            executeCopy(csvData, batch, failedNodeIds);
            return null;
        });
    }

    private String buildCsvRows(List<Map.Entry<UUID, Map.Entry<String, String>>> batch) {
        StringBuilder csv = new StringBuilder();
        for (Map.Entry<UUID, Map.Entry<String, String>> entry : batch) {
            UUID nodeId = entry.getKey();
            String metaKey = escapeCsv(entry.getValue().getKey());
            String metaValue = escapeCsv(entry.getValue().getValue());
            csv.append(nodeId).append('\t')
                    .append(metaKey).append('\t')
                    .append(metaValue).append('\n');
        }
        return csv.toString();
    }

    private String escapeCsv(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    private void executeCopy(String csvData, List<Map.Entry<UUID, Map.Entry<String, String>>> batch, ConcurrentHashMap<UUID, Boolean> failedNodeIds) {

        transactionTemplate.executeWithoutResult(status -> {
            DataSource dataSource = jdbcTemplate.getDataSource();
            if (dataSource == null) {
                log.error("DataSource is null");
                markBatchFailed(batch, failedNodeIds);
                throw new DataAccessException("DataSource is null") {};
            }

            Connection connection = DataSourceUtils.getConnection(dataSource);
            try {
                connection.setAutoCommit(false); // Ensure transaction control
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(COPY_TEMP_SQL);
                }

                try (InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8))) {
                    CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
                    copyManager.copyIn(
                            "COPY temp_node_metadata (node_id, meta_key, meta_value) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')",
                            inputStream
                    );
                }

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(UPSERT_METADATA_SQL);
                }

                connection.commit();
            } catch (SQLException | IOException e) {
                try {
                    connection.rollback();
                } catch (SQLException re) {
                    log.error("Rollback failed: {}", re.getMessage(), re);
                }
                log.error("COPY operation failed for {} metadata entries: {}", batch.size(), e.getMessage(), e);
                markBatchFailed(batch, failedNodeIds);
                throw new DataAccessException("COPY operation failed", e) {};
            } finally {
                DataSourceUtils.releaseConnection(connection, dataSource);
            }
        });
    }

    private void markBatchFailed(List<Map.Entry<UUID, Map.Entry<String, String>>> batch, ConcurrentHashMap<UUID, Boolean> failedNodeIds) {
        batch.forEach(entry -> failedNodeIds.put(entry.getKey(), true));
    }

    private void recordMetrics(int totalEntries, int failedCount, long startTime) {
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        log.info("Inserted {} metadata entries ({} failed) in {} ms", totalEntries - failedCount, failedCount, durationMs);
        meterRegistry.timer("node_metadata.insert.duration", Constant.OPS, Constant.NODES_METADATA)
                .record(durationMs, TimeUnit.MILLISECONDS);
        meterRegistry.counter("node_metadata.insert.processed", Constant.OPS, Constant.NODES_METADATA)
                .increment(totalEntries - failedCount);
        meterRegistry.counter("node_metadata.insert.failed", Constant.OPS, Constant.NODES_METADATA)
                .increment(failedCount);
    }
}