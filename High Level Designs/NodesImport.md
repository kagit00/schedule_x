# High-Level Design (HLD): Nodes Import Module

## 1. Overview

The Nodes Import Module ingests, validates, and persists node data from Kafka topics into a PostgreSQL database. It supports two ingestion patterns:

- **Cost-Based Imports**: Large-scale data from GZIP-compressed CSV files (e.g., user/node attributes).
- **Non-Cost-Based Imports**: Direct lists of reference IDs for lightweight processing.

The system leverages parallel batch processing, dead-letter queues (DLQ) for fault tolerance, and real-time metrics for monitoring.

## 2. Key Components

| Component                | Responsibilities                                                                 | Technologies                                      |
|--------------------------|----------------------------------------------------------------------------------|---------------------------------------------------|
| **Kafka Consumer**       | Listens to `.*-users` topics, processes messages, and routes failures to DLQ     | Spring Kafka, `@KafkaListener`, DLQ routing       |
| **Payload Processor**    | Validates and parses Kafka messages into `NodeExchange` objects                  | Spring `@Component`, JSON/GZIP parsing            |
| **Import Orchestrator**  | Routes valid payloads to cost-based or non-cost-based workflows; initializes job tracking | `ImportJobService`, `NodeImportValidator`         |
| **Batch Processing Engine** | Splits large payloads into batches; processes batches in parallel; aggregates results | `CompletableFuture`, `ThreadPoolTaskExecutor`     |
| **Storage Layer**        | Upserts nodes into PostgreSQL using efficient `COPY` commands; batches metadata inserts | PostgreSQL, Spring `JdbcTemplate`, `COPY`         |
| **Status Tracker**       | Updates job statuses (e.g., `PROCESSING`, `FAILED`) and emits metrics            | `NodesImportStatusUpdater`, Micrometer            |

## 3. Data Flow

1. **Ingestion**: Kafka messages are consumed from `.*-users` topics by `ScheduleXConsumer`.

2. **Validation & Parsing**:
    - `ScheduleXPayloadProcessor` checks for blank payloads
    - Parses valid messages into `NodeExchange` objects
    - Initiates async processing

3. **Routing**:
   | Path                  | Description                                                                 |
   |-----------------------|-----------------------------------------------------------------------------|
   | **Cost-Based Path**   | For file-based payloads (GZIP CSV), `ImportJobService` resolves the file, validates its structure, and streams batches |
   | **Non-Cost-Based Path** | For referenceIds lists, `ImportJobService` converts references to `Node` objects and partitions them into batches |

4. **Batch Processing**:
    - Batches are processed in parallel via a dedicated thread pool (`nodesImportExecutor`)
    - Each batch is validated, converted to `Node` objects, and persisted using PostgreSQL `COPY` for efficiency
    - Metadata is inserted in batches after node persistence

5. **Status & Metrics**:
    - `NodesImportStatusUpdater` tracks success/failure counts
    - Detects data discrepancies
    - Emits metrics (e.g., batch durations, conflict rates)

## 4. Fault Tolerance

- **Dead-Letter Queue (DLQ)**: Invalid messages are routed to `users-import-dlq` for reprocessing or auditing
- **Retries**: Transient failures trigger exponential backoff via Spring `RetryTemplate` (max 3 attempts)
- **Timeouts**: Batch processing timeouts (`node-timeout-ms`) fail stalled operations
- **Consistency Checks**: Validates parsed vs. processed node counts to detect discrepancies (e.g., `totalParsed != success.size() + failed.size()`)

## 5. Scalability

- **Concurrency**:
    - Kafka consumer parallelism (`concurrency=4`)
    - Dynamic thread pooling (`max-parallel-futures=4`)
- **Batch Optimization**: Adjustable `batch-size` (default: 1000) balances throughput and resource usage
- **Resource Isolation**: Dedicated executors (`nodesImportExecutor`) prevent blocking system threads

## 6. Technologies Used

| Category            | Technologies                                      |
|---------------------|---------------------------------------------------|
| **Stream Processing** | Spring Kafka, GZIP CSV parsing                    |
| **Data Persistence** | PostgreSQL (with `COPY` for bulk upserts), Spring `JdbcTemplate` |
| **Concurrency**     | Java `CompletableFuture`, Spring `ThreadPoolTaskExecutor` |
| **Observability**   | Micrometer metrics, SLF4J logging                 |
| **Resilience**      | Spring `RetryTemplate`, exponential backoff        |