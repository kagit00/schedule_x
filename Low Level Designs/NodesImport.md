---

# Low-Level Design (LLD): Nodes Import Module

---

### **1. Kafka Consumer (`ScheduleXConsumer`)**

**Configuration**:

```java
@Bean
public KafkaListenerConfig nodesImportConfig() {
    KafkaListenerConfig config = new KafkaListenerConfig();
    config.setTopicPattern(".*-users"); // Matches all topics ending with "-users"
    config.setGroupId("nodes-import-group");
    config.setConcurrency(4); // 4 consumer threads
    config.setDlqTopic("users-import-dlq");
    config.setPayloadProcessor(payloadProcessor::processImportedNodesPayload); // Delegates to processor
    return config;
}
```

**Message Consumption**:

```java
@KafkaListener(
    topicPattern = "#{@nodesImportConfig.topicPattern}",
    groupId = "#{@nodesImportConfig.groupId}",
    concurrency = "#{@nodesImportConfig.concurrency}",
    containerFactory = "kafkaListenerContainerFactory"
)
public void consumeNodesImport(ConsumerRecord<String, String> consumerRecord) {
    consume(consumerRecord, getListenerConfigs().get(0)); // Uses DLQ on failure
}
```

**DLQ Handling**:

* Failed messages are routed to `users-import-dlq` with original headers and a `__TypeId__` header for traceability.
* Retry logic is handled at the `RetryTemplate` level (see Section 7).

---

### **2. Payload Processing (`ScheduleXPayloadProcessor`)**

**Validation & Parsing**:

```java
public CompletableFuture<Void> processImportedNodesPayload(String payload) {
    if (payload == null || payload.isBlank()) {
        log.warn("Skipping processing: Blank payload");
        return CompletableFuture.completedFuture(null);
    }

    NodeExchange parsedPayload = BasicUtility.safeParse(payload, NodeExchange.class);
    if (parsedPayload == null) {
        log.warn("Failed to parse payload: {}", payload);
        return CompletableFuture.completedFuture(null);
    }

    return importJobService.startNodesImport(parsedPayload)
            .exceptionally(ex -> { // Async error handling
                log.error("Error processing parsed payload for groupId={}", parsedPayload.getGroupId(), ex);
                return null;
            });
}
```

**NodeExchange Structure**:

```java
public class NodeExchange {
    private String groupId;
    private UUID domainId;
    private MultipartFile file; // For cost-based
    private List<String> referenceIds; // For non-cost-based
    // Getters, setters, and validation logic
}
```

---

### **3. Import Orchestration (`ImportJobServiceImpl`)**

**Payload Validation**:

```java
public boolean isValidPayloadForCostBasedNodes(NodeExchange payload) {
    return payload.getFile() != null && !payload.getFile().isEmpty() &&
           payload.getReferenceIds() == null; // Ensure only one input type
}

public boolean isValidPayloadForNonCostBasedNodes(NodeExchange payload) {
    return payload.getFile() == null &&
           payload.getReferenceIds() != null && !payload.getReferenceIds().isEmpty();
}
```

**Job Initialization**:

```java
@Override
public CompletableFuture<Void> startNodesImport(NodeExchange payload) {
    UUID jobId = statusUpdater.initiateNodesImport(payload); // Generates job ID and updates DB

    if (isValidPayloadForCostBasedNodes(payload)) {
        MultipartFile file = payload.getFile();
        FileValidationUtility.validateInput(file, payload.getGroupId()); // Checks file size, format
        return nodesImportService.processNodesImport(jobId, file, payload);

    } else if (isValidPayloadForNonCostBasedNodes(payload)) {
        return nodesImportService.processNodesImport(jobId, payload.getReferenceIds(),
                payload.getGroupId(), batchSize, payload.getDomainId());

    } else {
        log.warn("Invalid payload for groupId={}: {}", payload.getGroupId(), payload);
        return CompletableFuture.completedFuture(null);
    }
}
```

---

### **4. Batch Processing Engine (`NodesImportService`)**

**Cost-Based Processing**:

```java
public CompletableFuture<Void> processNodesImport(UUID jobId, MultipartFile file, NodeExchange message) {
    UUID domainId = message.getDomainId();
    MatchingGroup group = groupConfigService.getGroupConfig(message.getGroupId(), domainId);

    return CompletableFuture.supplyAsync(() -> { // Offloads to executor
        logImportStart(jobId, group.getId().toString(), domainId, file);
        statusUpdater.updateJobStatus(jobId, JobStatus.PROCESSING);

        long startTime = System.nanoTime();
        AtomicInteger totalParsed = new AtomicInteger(0);
        List<String> success = Collections.synchronizedList(new ArrayList<>());
        List<String> failed = Collections.synchronizedList(new ArrayList<>());

        try (InputStream gzipStream = new GZIPInputStream(file.getInputStream())) {
            processBatchesFromStream(jobId, message, gzipStream, totalParsed, success, failed, startTime);
        } catch (IOException e) {
            handleImportFailure(jobId, domainId, group.getGroupId(), e);
            return null;
        }

        finalizeJob(jobId, group.getGroupId(), domainId, success, failed, totalParsed.get(), startTime);
        return null;
    }, executor).handle((result, throwable) -> { // Global error handling
        if (throwable != null) {
            log.error("Failed to process nodes import for jobId={}: {}", jobId, throwable.getMessage(), throwable);
            statusUpdater.handleUnexpectedFailure(jobId, domainId, group.getGroupId(), throwable);
        }
        return null;
    });
}
```

**Stream Batching Algorithm**:

```java
private void processBatchesFromStream(UUID jobId, NodeExchange message,
                                      InputStream gzipStream, AtomicInteger totalParsed,
                                      List<String> success, List<String> failed, long startTime) {
    List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());
    int maxFutures = maxParallelFutures; // Configurable (default: 4)

    CsvParser.parseInBatches(gzipStream, nodeResponseFactory, batch -> {
        futures.add(processBatchAsync(jobId, batch, message, success, failed, totalParsed, startTime));
        if (futures.size() >= maxFutures) {
            joinAndClearFutures(futures); // Throttle parallelism
        }
    });

    joinAndClearFutures(futures); // Process remaining futures
}
```

**Batch Processing with Timeout**:

```java
private CompletableFuture<Void> processBatchAsync(UUID jobId, List<NodeResponse> batch, NodeExchange message,
                                                  List<String> success, List<String> failed,
                                                  AtomicInteger totalParsed, long startTime) {
    return CompletableFuture.runAsync(() -> {
        try {
            nodesImportProcessor.processBatch(jobId, batch, message, success, failed, totalParsed);
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            log.info("Job {}: Completed batch of {} nodes in {} ms", jobId, batch.size(), durationMs);
        } catch (Exception e) {
            log.error("Job {}: Batch failed: {}", jobId, e.getMessage(), e);
            batch.forEach(r -> failed.add(r.getReferenceId()));
            throw new InternalServerErrorException("Batch processing failed for jobId=" + jobId + ": " + e.getMessage());
        }
    }, executor).whenComplete((result, throwable) -> {
        if (throwable != null) {
            if (throwable instanceof TimeoutException) {
                log.error("Job {}: Batch processing timed out after {} ms", jobId, nodeTimeoutMs);
                batch.forEach(r -> failed.add(r.getReferenceId()));
            } else {
                log.error("Job {}: Batch processing failed: {}", jobId, throwable.getMessage(), throwable);
            }
        }
    }).orTimeout(nodeTimeoutMs, TimeUnit.MILLISECONDS); // Configurable timeout (default: 500ms)
}
```

**Non-Cost-Based Processing**:

```java
public CompletableFuture<Void> processNodesImport(UUID jobId, List<String> referenceIds, String groupId, int batchSize, UUID domainId) {
    MatchingGroup group = groupConfigService.getGroupConfig(groupId, domainId);

    return CompletableFuture.supplyAsync(() -> {
        log.info("Job {} started for groupId={}, domainId={}, referenceIds count={}", jobId, groupId, domainId, referenceIds.size());
        statusUpdater.updateJobStatus(jobId, JobStatus.PROCESSING);

        if (referenceIds.isEmpty()) {
            statusUpdater.failJob(jobId, groupId, "No referenceIds provided.", List.of(), List.of(), 0, domainId);
            return null;
        }

        statusUpdater.updateTotalNodes(jobId, referenceIds.size());

        List<Node> nodes = GraphRequestFactory.createNodesFromReferences(referenceIds, group.getId(), NodeType.USER, domainId);
        log.info("Job {}: Created {} nodes from referenceIds", jobId, nodes.size());

        List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());
        List<List<Node>> batches = ListUtils.partition(nodes, batchSize);

        for (List<Node> batch : batches) {
            log.info("Job {}: Submitting batch of size {}", jobId, batch.size());
            futures.add(CompletableFuture.runAsync(() -> {
                nodesImportProcessor.processAndPersist(jobId, groupId, batch, batchSize, referenceIds.size(), domainId);
                log.info("Job {}: Completed batch of size {}", jobId, batch.size());
            }, executor).orTimeout(nodeTimeoutMs, TimeUnit.MILLISECONDS));
        }

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenAcceptAsync(v -> resultFuture.complete(null), executor)
                .exceptionally(t -> {
                    log.error("Failed to complete futures for jobId={}: {}", jobId, t.getMessage());
                    resultFuture.completeExceptionally(t);
                    return null;
                });

        try {
            resultFuture.get(nodeTimeoutMs, TimeUnit.MILLISECONDS); // Global timeout
        } catch (Exception e) {
            log.error("Error waiting for futures for jobId={}: {}", jobId, e.getMessage());
            throw new RuntimeException(e);
        }
        return null;
    }, executor).handle((result, throwable) -> {
        if (throwable != null) {
            log.error("Failed to process nodes import for jobId={}: {}", jobId, throwable.getMessage(), throwable);
            statusUpdater.handleUnexpectedFailure(jobId, domainId, groupId, new RuntimeException(throwable));
            meterRegistry.counter("node_import_errors", "domainId", domainId.toString(), "groupId", groupId).increment();
        }
        return null;
    });
}
```

---

### **5. Storage Layer (`NodesStorageProcessor`)**

**Upsert Mechanism**:

```java
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
    }).orTimeout(timeoutMs, TimeUnit.MILLISECONDS); // Configurable (default: 50s)
}
```

**Batch Upsert with PostgreSQL `COPY`**:

```java
private ConcurrentHashMap<String, UUID> upsertNodes(List<Node> nodes) {
    ConcurrentHashMap<String, UUID> refIdToNodeId = new ConcurrentHashMap<>();
    List<List<Node>> batches = ListUtils.partition(nodes, batchSize); // batchSize from config

    for (List<Node> batch : batches) {
        retryTemplate.execute(context -> { // Exponential backoff
            upsertBatch(batch, refIdToNodeId);
            return null;
        });
    }

    return refIdToNodeId;
}

private void upsertBatch(List<Node> batch, ConcurrentHashMap<String, UUID> refIdToNodeId) {
    long startTime = System.nanoTime();
    String csvData = buildCsvRows(batch); // Formats batch as TSV
    List<Map<String, Object>> results = executeCopy(csvData); // Executes COPY command

    int fetched = 0;
    for (Map<String, Object> row : results) {
        UUID nodeId = (UUID) row.get("id");
        String key = row.get(HeaderNormalizer.FIELD_REFERENCE_ID) + ":" + row.get(HeaderNormalizer.FIELD_GROUP_ID);
        refIdToNodeId.put(key, nodeId);
        fetched++;
    }

    recordMetrics(batch.size(), fetched, startTime);
}
```

**COPY Command Execution**:

```java
private List<Map<String, Object>> executeCopy(String csvData) {
    return transactionTemplate.execute(status -> {
        DataSource dataSource = jdbcTemplate.getDataSource();
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            connection.setAutoCommit(false);
            // 1. Create temp table
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(QueryUtils.getNodesTempTableSQL());
            }
            // 2. Copy data from CSV to temp table
            try (InputStream inputStream = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8))) {
                CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
                copyManager.copyIn(
                    "COPY temp_nodes (id, reference_id, group_id, type, domain_id, created_at) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')",
                    inputStream
                );
            }
            // 3. Upsert into main table and return results
            List<Map<String, Object>> results = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(QueryUtils.getNodesUpsertSQL());
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
```

**Upsert SQL**:

```sql
-- QueryUtils.getNodesUpsertSQL()
WITH upsert AS (
    UPDATE nodes n
    SET
        reference_id = EXCLUDED.reference_id,
        group_id = EXCLUDED.group_id,
        type = EXCLUDED.type,
        domain_id = EXCLUDED.domain_id,
        created_at = EXCLUDED.created_at
    FROM temp_nodes
    WHERE n.reference_id = temp_nodes.reference_id AND n.group_id = temp_nodes.group_id
    RETURNING n.id, temp_nodes.reference_id, temp_nodes.group_id
)
INSERT INTO nodes (id, reference_id, group_id, type, domain_id, created_at)
SELECT
    gen_random_uuid(), -- Or use provided UUID if not null
    reference_id,
    group_id,
    type,
    domain_id,
    created_at
FROM temp_nodes
WHERE NOT EXISTS (
    SELECT 1 FROM upsert
    WHERE upsert.reference_id = temp_nodes.reference_id
      AND upsert.group_id = temp_nodes.group_id
)
RETURNING id, reference_id, group_id;
```

---

### **6. Status Tracking (`NodesImportStatusUpdater`)**

**Database Schema (PostgreSQL)**:

```sql
CREATE TABLE job_status (
    id UUID PRIMARY KEY,
    group_id VARCHAR(255) NOT NULL,
    domain_id UUID NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    total_nodes INT DEFAULT 0,
    success_count INT DEFAULT 0,
    failed_count INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_job_status_group_domain ON job_status (group_id, domain_id);
```

**Status Update Operations**:

```java
public UUID initiateNodesImport(NodeExchange payload) {
    UUID jobId = UUID.randomUUID();
    String groupId = payload.getGroupId();
    UUID domainId = payload.getDomainId();

    jdbcTemplate.update(
        "INSERT INTO job_status (id, group_id, domain_id, status, created_at, updated_at) " +
        "VALUES (?, ?, ?, 'PENDING', NOW(), NOW())",
        jobId, groupId, domainId
    );
    return jobId;
}

public void updateJobStatus(UUID jobId, JobStatus status) {
    jdbcTemplate.update(
        "UPDATE job_status SET status = ?, updated_at = NOW() WHERE id = ?",
        status.name(), jobId
    );
}

public void updateTotalNodes(UUID jobId, int totalNodes) {
    jdbcTemplate.update(
        "UPDATE job_status SET total_nodes = ? WHERE id = ?",
        totalNodes, jobId
    );
}

public void completeJob(UUID jobId, String groupId, List<String> success, int total, UUID domainId) {
    int successCount = success.size();
    int failedCount = total - successCount;

    jdbcTemplate.update(
        "UPDATE job_status SET status = 'COMPLETED', success_count = ?, failed_count = ?, updated_at = NOW() WHERE id = ?",
        successCount, failedCount, jobId
    );
    // Emit success metric
    meterRegistry.counter("node_import_success", "domainId", domainId.toString(), "groupId", groupId).increment(successCount);
}

public void failJob(UUID jobId, String groupId, String error, List<String> success, List<String> failed, int total, UUID domainId) {
    int successCount = success.size();
    int failedCount = failed.size();

    jdbcTemplate.update(
        "UPDATE job_status SET status = 'FAILED', success_count = ?, failed_count = ?, updated_at = NOW() WHERE id = ?",
        successCount, failedCount, jobId
    );
    // Log error and emit failure metric
    log.error("Job {} failed: {}. Success: {}, Failed: {}", jobId, error, successCount, failedCount);
    meterRegistry.counter("node_import_failures", "domainId", domainId.toString(), "groupId", groupId).increment(failedCount);
}
```

---

### **7. Fault Tolerance**

**Retry Configuration**:

```java
@Bean
public RetryTemplate retryTemplate() {
    RetryTemplate template = new RetryTemplate();
    ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
    backOffPolicy.setInitialInterval(1000); // 1st retry: 1s
    backOffPolicy.setMultiplier(2.0); // 2nd retry: 2s, 3rd: 4s
    backOffPolicy.setMaxInterval(10000); // Max 10s
    template.setBackOffPolicy(backOffPolicy);

    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(3); // Max 3 retries
    template.setRetryPolicy(retryPolicy);
    return template;
}
```

**Retry Usage in Storage Layer**:

```java
private void upsertBatch(List<Node> batch, ConcurrentHashMap<String, UUID> refIdToNodeId) {
    retryTemplate.execute(context -> { // Applies retry logic
        // ... (COPY command execution)
        return null;
    });
}
```

---

### **8. Concurrency and Resource Management**

**Thread Pool Configuration**:

```java
@Bean(name = "nodesImportExecutor")
public ThreadPoolTaskExecutor nodesImportExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(4); // Matches Kafka concurrency
    executor.setMaxPoolSize(8); // Scales under load
    executor.setQueueCapacity(100); // Backpressure: reject if queue full
    executor.setThreadNamePrefix("nodes-import-");
    executor.initialize();
    return executor;
}
```

**Backpressure Handling**:

* If the executor’s queue is full, `CompletableFuture.runAsync` throws `TaskRejectedException`.
* This exception is caught in the global error handler, updating the job status to `FAILED` and incrementing the `node_import_errors` metric.

---

### **9. Observability**

**Micrometer Metrics**:

```java
// Track batch processing times
meterRegistry.timer("node_import_batch_duration", Constant.OPS, Constant.NODES).record(durationMs, TimeUnit.MILLISECONDS);

// Track conflict rates during upserts
meterRegistry.counter("node_import_conflict_updates", Constant.OPS, Constant.NODES).increment(batchSize - fetched);

// Track metadata insertion times
meterRegistry.timer("node_import_metadata_duration", Constant.OPS, Constant.NODES_METADATA).record(durationMs, TimeUnit.MILLISECONDS);
```

**Log Correlation**:

* All logs include `jobId` for traceability (e.g., `log.info("Job {}: Processing batch", jobId)`).

---

## Notes

This LLD provides a **granular, implementation-focused view** of the Nodes Import Module, balancing technical depth with readability for recruiters and interviewers. For interviews, be prepared to discuss trade-offs (e.g., `COPY` vs. `INSERT` for PostgreSQL, thread pool sizing) and failure scenarios.
sStorageProcessor ..> NodeMetadataBatchWriter : batchInsertMetadata