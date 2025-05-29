package com.shedule.x.processors;

import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Node;
import com.shedule.x.utils.basic.Constant;
import com.shedule.x.utils.media.csv.HeaderNormalizer;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class NodesStorageProcessor {
    private static final String COPY_TEMP_SQL =
            "CREATE TEMP TABLE temp_nodes (\n" +
                    "id UUID, \n" +
                    "reference_id TEXT, \n" +
                    "group_id TEXT, \n" +
                    "type TEXT, \n" +
                    "domain_id UUID, \n" +
                    "created_at TIMESTAMP) \n" +
                    "ON COMMIT DROP";

    private static final String UPSERT_NODES_SQL =
            "INSERT INTO public.nodes (id, reference_id, group_id, type, domain_id, created_at) \n" +
                    "SELECT id, reference_id, group_id, type, domain_id, created_at FROM temp_nodes \n" +
                    "ON CONFLICT (reference_id, group_id) DO UPDATE SET \n" +
                    "type = EXCLUDED.type, domain_id = EXCLUDED.domain_id, created_at = EXCLUDED.created_at \n" +
                    "RETURNING id, reference_id, group_id";


    private final JdbcTemplate jdbcTemplate;
    private final NodeMetadataBatchWriter metadataBatchWriter;
    private final MeterRegistry meterRegistry;
    private final TransactionTemplate transactionTemplate;
    private final RetryTemplate retryTemplate;
    private final ThreadPoolTaskExecutor nodesImportExecutor;

    @Value("${import.batch-size}")
    private int batchSize;

    @Value("${nodes.import.timeout-ms:50000}")
    private long timeoutMs;

    public NodesStorageProcessor(
            JdbcTemplate jdbcTemplate,
            NodeMetadataBatchWriter metadataBatchWriter,
            MeterRegistry meterRegistry,
            TransactionTemplate transactionTemplate,
            RetryTemplate retryTemplate,
            @Qualifier("nodesImportExecutor") ThreadPoolTaskExecutor nodesImportExecutor
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.metadataBatchWriter = metadataBatchWriter;
        this.meterRegistry = meterRegistry;
        this.transactionTemplate = transactionTemplate;
        this.retryTemplate = retryTemplate;
        this.nodesImportExecutor = nodesImportExecutor;
    }

    public CompletableFuture<Void> saveNodesSafely(List<Node> nodes) {
        if (nodes.isEmpty()) {
            log.warn("Empty node list, skipping upsert.");
            return CompletableFuture.completedFuture(null);
        }

        List<Node> processedNodes = nodes.stream()
                .map(node -> Node.builder()
                        .id(node.getId() != null ? node.getId() : UUID.randomUUID())
                        .referenceId(node.getReferenceId())
                        .groupId(node.getGroupId())
                        .type(node.getType())
                        .domainId(node.getDomainId())
                        .createdAt(node.getCreatedAt())
                        .metaData(node.getMetaData())
                        .build())
                .collect(Collectors.toList());

        long startTime = System.nanoTime();
        return CompletableFuture.runAsync(() -> {
            ConcurrentHashMap<String, UUID> refIdToNodeId = upsertNodes(processedNodes);
            insertMetadata(processedNodes, refIdToNodeId.values());
        }, nodesImportExecutor).whenComplete((result, throwable) -> {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            if (throwable != null) {
                log.error("Node processing failed: {}", throwable.getMessage(), throwable);
                meterRegistry.counter("node_import_errors", Constant.OPS, Constant.NODES).increment();
                throw new InternalServerErrorException("Node processing failed: " + throwable.getMessage());
            }
            log.info("Processed batch of {} nodes in {} ms", nodes.size(), durationMs);
            meterRegistry.timer("node_import_total_duration", Constant.OPS, Constant.NODES)
                    .record(durationMs, TimeUnit.MILLISECONDS);
        }).orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private ConcurrentHashMap<String, UUID> upsertNodes(List<Node> nodes) {
        ConcurrentHashMap<String, UUID> refIdToNodeId = new ConcurrentHashMap<>();
        List<List<Node>> batches = ListUtils.partition(nodes, batchSize);

        for (List<Node> batch : batches) {
            retryTemplate.execute(context -> {
                upsertBatch(batch, refIdToNodeId);
                return null;
            });
        }

        return refIdToNodeId;
    }

    private void upsertBatch(List<Node> batch, ConcurrentHashMap<String, UUID> refIdToNodeId) {
        long startTime = System.nanoTime();
        String csvData = buildCsvRows(batch);
        List<Map<String, Object>> results = executeCopy(csvData);

        int fetched = 0;
        for (Map<String, Object> row : results) {
            UUID nodeId = (UUID) row.get("id");
            String key = row.get(HeaderNormalizer.FIELD_REFERENCE_ID) + ":" + row.get(HeaderNormalizer.FIELD_GROUP_ID);
            refIdToNodeId.put(key, nodeId);
            fetched++;
        }

        recordMetrics(batch.size(), fetched, startTime);
    }

    private String buildCsvRows(List<Node> batch) {
        StringBuilder csv = new StringBuilder();
        for (Node node : batch) {
            UUID nodeId = node.getId() != null ? node.getId() : UUID.randomUUID();
            csv.append(nodeId).append('\t')
                    .append(node.getReferenceId()).append('\t')
                    .append(node.getGroupId()).append('\t')
                    .append(node.getType()).append('\t')
                    .append(node.getDomainId() != null ? node.getDomainId() : "").append('\t')
                    .append(node.getCreatedAt().toString().replace(" ", "T")).append('\n');
        }
        return csv.toString();
    }

    private List<Map<String, Object>> executeCopy(String csvData) {
        return transactionTemplate.execute(status -> {
            DataSource dataSource = jdbcTemplate.getDataSource();
            if (dataSource == null) {
                log.error("DataSource is null");
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
                            "COPY temp_nodes (id, reference_id, group_id, type, domain_id, created_at) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')",
                            inputStream
                    );
                }

                List<Map<String, Object>> results = new ArrayList<>();
                try (PreparedStatement ps = connection.prepareStatement(UPSERT_NODES_SQL);
                     ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            row.put(metaData.getColumnLabel(i), rs.getObject(i));
                        }
                        results.add(row);
                    }
                }

                connection.commit();
                return results;
            } catch (SQLException | IOException e) {
                try {
                    connection.rollback();
                } catch (SQLException re) {
                    log.error("Rollback failed: {}", re.getMessage(), re);
                }
                log.error("COPY operation failed: {}", e.getMessage(), e);
                throw new DataAccessException("COPY operation failed", e) {};
            } finally {
                DataSourceUtils.releaseConnection(connection, dataSource);
            }
        });
    }

    private void recordMetrics(int batchSize, int fetched, long startTime) {
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        log.info("COPY upserted {} nodes ({} conflicts) in {} ms", batchSize, batchSize - fetched, durationMs);
        meterRegistry.timer("node_import_batch_duration", Constant.OPS, Constant.NODES)
                .record(durationMs, TimeUnit.MILLISECONDS);
        meterRegistry.counter("node_import_batch_processed", Constant.OPS, Constant.NODES).increment(batchSize);
        meterRegistry.counter("node_import_conflict_updates", Constant.OPS, Constant.NODES).increment(batchSize - (double) fetched);
    }

    private void insertMetadata(List<Node> nodes, Collection<UUID> validNodeIds) {
        Map<UUID, Map<String, String>> metadataMap = nodes.stream()
                .filter(node -> validNodeIds.contains(node.getId()) && node.getMetaData() != null && !node.getMetaData().isEmpty())
                .collect(Collectors.toMap(Node::getId, Node::getMetaData));

        if (metadataMap.isEmpty()) {
            log.debug("No metadata to insert");
            return;
        }

        long startTime = System.nanoTime();
        try {
            metadataBatchWriter.batchInsertMetadata(metadataMap);
        } catch (Exception e) {
            log.warn("Metadata insertion failed for {} nodes: {}", metadataMap.size(), e.getMessage());
        }
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        log.info("Processed metadata for {} nodes in {} ms", metadataMap.size(), durationMs);
        meterRegistry.timer("node_import_metadata_duration", Constant.OPS, Constant.NODES_METADATA)
                .record(durationMs, TimeUnit.MILLISECONDS);
        meterRegistry.counter("node_import_batch_processed", Constant.OPS, Constant.NODES_METADATA)
                .increment(metadataMap.size());
    }
}