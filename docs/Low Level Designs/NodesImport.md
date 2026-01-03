# Node Import System - Low-Level Design Document



---

This document describes the low-level design and reference implementation of a node import pipeline, focusing on correctness, data flow, and concurrency patterns rather than production deployment, tuning, or observed performance.

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Design](#2-architecture-design)
3. [Component Design](#3-component-design)
4. [Data Flow Architecture](#4-data-flow-architecture)
5. [Kafka Integration](#5-kafka-integration)
6. [File Processing Engine](#6-file-processing-engine)
7. [Storage Architecture](#7-storage-architecture)
8. [Error Handling & Resilience](#8-error-handling--resilience)

---

## 1. System Overview

### 1.1 Purpose

The **Node Import System** is an event-driven data ingestion component that processes entity (node) imports from messaging topics. It supports both file-based imports (GZIP CSV) from object storage or filesystem and reference-based imports (list of IDs), with error handling and PostgreSQL bulk loading via COPY protocol.

### 1.2 Key Capabilities

```mermaid
flowchart TB
    ROOT["Node Import<br/>System"]

    ROOT --> ED["Event-Driven"]
    ROOT --> FP["File Processing"]
    ROOT --> BI["Batch Import"]
    ROOT --> ST["Storage"]
    ROOT --> RS["Resilience"]

    ED --> ED1["Kafka Consumers"]
    ED --> ED2["Multiple Topics"]
    ED --> ED3["Error Handling"]
    ED --> ED4["Status Publishing"]

    FP --> FP1["GZIP CSV Parsing"]
    FP --> FP2["Object Storage Integration"]
    FP --> FP3["Filesystem Support"]
    FP --> FP4["Streaming Processing"]

    BI --> BI1["Reference Lists"]
    BI --> BI2["Bulk Operations"]
    BI --> BI3["Parallel Processing"]

    ST --> ST1["PostgreSQL COPY"]
    ST --> ST2["Metadata Handling"]
    ST --> ST3["Transaction Safety"]
    ST --> ST4["Conflict Resolution"]

    RS --> RS1["Retry Mechanisms"]
    RS --> RS2["Timeout Management"]
    RS --> RS3["Status Tracking"]
    RS --> RS4["Error Recovery"]

    style ROOT fill:#ECEFF1
    style ED fill:#E3F2FD
    style FP fill:#E8F5E9
    style BI fill:#FFFDE7
    style ST fill:#FCE4EC
    style RS fill:#EDE7F6

```

---

## 2. Architecture Design

### 2.1 Logical Architecture

```mermaid
graph TB
    subgraph "Event Layer"
        A1[Kafka Consumers<br/>Topic Pattern Matching]
        A2[Message Routing<br/>Error Handling]
    end
    
    subgraph "Processing Layer"
        B1[Payload Processor<br/>Message Parsing]
        B2[Import Job Service<br/>Job Orchestration]
        B3[File Processing Engine<br/>CSV Streaming]
        B4[Batch Processing Engine<br/>Reference Lists]
    end
    
    subgraph "Storage Layer"
        C1[Nodes Import Processor<br/>Batch Handling]
        C2[Nodes Storage Processor<br/>PostgreSQL COPY]
        C3[Metadata Batch Writer<br/>Bulk Metadata Insert]
    end
    
    subgraph "State Management"
        D1[Status Updater<br/>Job Lifecycle]
        D2[Status Publisher<br/>Messaging Producer]
    end
    
    subgraph "Infrastructure"
        E1[(PostgreSQL<br/>Nodes + Metadata)]
        E2[(Object Storage<br/>File Storage)]
        E3[Messaging System<br/>Topics]
    end
    
    A1 --> B1
    A2 --> B1
    B1 --> B2
    B2 --> B3
    B2 --> B4
    B3 --> C1
    B4 --> C1
    C1 --> C2
    C2 --> C3
    
    B2 --> D1
    C1 --> D1
    D1 --> D2
    
    C2 --> E1
    C3 --> E1
    B3 --> E2
    A1 --> E3
    D2 --> E3
    
    style A1 fill:#4CAF50
    style B2 fill:#2196F3
    style C2 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#607D8B
```

### 2.2 Component Architecture

```mermaid
C4Container
    title Container Diagram - Node Import System
    
    Container_Boundary(app, "Import Application") {
        Container(consumer, "Kafka Consumer", "Spring Kafka", "Consumes import messages")
        Container(processor, "Payload Processor", "Java", "Parses & validates messages")
        Container(jobsvc, "Import Job Service", "Java", "Orchestrates import jobs")
        Container(filesvc, "File Processing Service", "Java", "Streams GZIP CSV files")
        Container(batchsvc, "Batch Processing Service", "Java", "Handles reference lists")
        Container(storage, "Storage Processor", "Java", "PostgreSQL COPY protocol")
        Container(status, "Status Updater", "Java", "Manages job status")
    }
    
    ContainerDb(postgres, "PostgreSQL", "Relational DB", "Nodes, Metadata, Jobs")
    ContainerDb(storage, "Object Storage", "Storage", "CSV files")
    Container(messaging, "Messaging System", "Message Broker", "Import topics + error handling")
    
    Rel(consumer, processor, "Routes messages", "In-process")
    Rel(processor, jobsvc, "Initiates import", "Async")
    Rel(jobsvc, filesvc, "File-based import", "Async")
    Rel(jobsvc, batchsvc, "Reference-based import", "Async")
    Rel(filesvc, storage, "Batches nodes", "Async")
    Rel(batchsvc, storage, "Batches nodes", "Async")
    Rel(storage, postgres, "Bulk insert", "COPY Protocol")
    Rel(jobsvc, status, "Update status", "Sync")
    Rel(status, messaging, "Publish status", "Producer")
    Rel(filesvc, storage, "Access file", "API")
    
    UpdateLayoutConfig($c4ShapeInRow="3")
```

---

## 3. Component Design

### 3.1 Kafka Consumer Component

```mermaid
classDiagram
    class ScheduleXConsumer {
        -ScheduleXProducer dlqProducer
        -ThreadPoolTaskExecutor taskExecutor
        +consumeNodesImport(ConsumerRecord) void
        +consumeJobStatus(ConsumerRecord) void
    }
    
    class BaseKafkaConsumer {
        <<abstract>>
        -ScheduleXProducer dlqProducer
        -ThreadPoolTaskExecutor taskExecutor
        -List~KafkaListenerConfig~ listenerConfigs
        #consume(ConsumerRecord, KafkaListenerConfig) void
        -sendToDlq(KafkaListenerConfig, ConsumerRecord) void
        #getListenerConfigs() List~KafkaListenerConfig~
    }
    
    class KafkaListenerConfig {
        -String topicPattern
        -String groupId
        -PayloadProcessor payloadProcessor
    }
    
    class PayloadProcessor {
        <<interface>>
        +process(String) CompletableFuture~Void~
    }
    
    ScheduleXConsumer --|> BaseKafkaConsumer
    BaseKafkaConsumer --> KafkaListenerConfig : uses
    KafkaListenerConfig --> PayloadProcessor : delegates
```

**Consumer Flow**:

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant Consumer as ScheduleXConsumer
    participant Base as BaseKafkaConsumer
    participant Processor as PayloadProcessor
    participant Executor as Thread Pool
    
    Kafka->>Consumer: ConsumerRecord (users topic)
    Consumer->>Base: consume(record, config)
    
    alt Payload is null/blank
        Base->>Base: Error handling
    else Payload valid
        Base->>Executor: runAsync(() -> process(payload))
        
        par Async Processing
            Executor->>Processor: process(payload).get()
            
            alt Processing succeeds
                Processor-->>Executor: Success
            else Processing fails
                Processor-->>Executor: Exception
            end
        end
        
        Base->>Base: orTimeout(configurable)
    end
```

### 3.2 Payload Processor Component

```mermaid
classDiagram
    class ScheduleXPayloadProcessor {
        -ImportJobService importJobService
        +processImportedNodesPayload(String) CompletableFuture~Void~
        +processNodesImportJobStatusPayload(String) CompletableFuture~Void~
    }
    
    class NodeExchange {
        +UUID domainId
        +String groupId
        +String filePath
        +String fileName
        +String contentType
        +List~String~ referenceIds
    }
    
    class NodesTransferJobExchange {
        +UUID jobId
        +String groupId
        +UUID domainId
        +String status
        +int processed
        +int total
        +List~String~ successList
        +List~String~ failedList
    }
    
    ScheduleXPayloadProcessor ..> NodeExchange : parses
    ScheduleXPayloadProcessor ..> NodesTransferJobExchange : parses
```

**Payload Processing Decision Tree**:

```mermaid
flowchart TD
    A[Receive Kafka Message] --> B{Payload Empty?}
    B -->|Yes| C[Error handling]
    B -->|No| D[Parse JSON to Exchange Object]
    
    D --> E{Parsing Success?}
    E -->|No| F[Error handling]
    E -->|Yes| G{Message Type?}
    
    G -->|Node Import| H[processImportedNodesPayload]
    G -->|Job Status| I[processNodesImportJobStatusPayload]
    
    H --> J[Call importJobService.startNodesImport]
    I --> K[Job status handling]
    
    style A fill:#4CAF50
    style D fill:#2196F3
    style H fill:#FF9800
    style I fill:#9C27B0
```

### 3.3 Import Job Service Component

```mermaid
classDiagram
    class ImportJobServiceImpl {
        -NodesImportService nodesImportService
        -NodesImportStatusUpdater statusUpdater
        +startNodesImport(NodeExchange) CompletableFuture~Void~
    }
    
    class NodesImportService {
        -GroupConfigService groupConfigService
        -NodesImportStatusUpdater statusUpdater
        -NodesImportProcessor nodesImportProcessor
        -ThreadPoolTaskExecutor executor
        +processNodesImport(UUID, MultipartFile, NodeExchange) CompletableFuture~Void~
        +processNodesImport(UUID, List~String~, String, int, UUID) CompletableFuture~Void~
        -processBatchesFromStream(...) void
        -processBatchAsync(...) CompletableFuture~Void~
    }
    
    class NodeImportValidator {
        <<utility>>
        +isValidPayloadForCostBasedNodes(NodeExchange) boolean
        +isValidPayloadForNonCostBasedNodes(NodeExchange) boolean
    }
    
    ImportJobServiceImpl --> NodesImportService
    ImportJobServiceImpl --> NodeImportValidator : validates
```

**Import Type Decision Logic**:

```mermaid
flowchart TD
    A[NodeExchange Received] --> B{Validation Check}
    
    B -->|Cost-Based Validation| C[Has filePath?]
    C -->|Yes| D[Has fileName?]
    D -->|Yes| E[File-Based Import Path]
    
    B -->|Non-Cost-Based Validation| F[Has groupId?]
    F -->|Yes| G[Has referenceIds?]
    G -->|Yes & Not Empty| H[Has domainId?]
    H -->|Yes| I[Reference-Based Import Path]
    
    E --> J[Create Job Status]
    I --> J
    
    J --> K[Resolve MultipartFile]
    J --> L[Create Nodes from References]
    
    K --> M{File Source?}
    M -->|URL| N[RemoteMultipartFile]
    M -->|Local Path| O[FileSystemMultipartFile]
    
    N --> P[Validate File]
    O --> P
    
    P --> Q[nodesImportService.processNodesImport<br/>File + NodeExchange]
    
    L --> R[Partition References]
    R --> S[nodesImportService.processNodesImport<br/>References + GroupId]
    
    C -->|No| T[Invalid Payload]
    D -->|No| T
    G -->|No or Empty| T
    H -->|No| T
    
    T --> U[Error handling]
    
    style E fill:#C8E6C9
    style I fill:#BBDEFB
    style N fill:#FFF9C4
    style O fill:#FFF9C4
    style T fill:#FFCDD2
```

---

## 4. Data Flow Architecture

### 4.1 End-to-End Data Flow

```mermaid
flowchart TB
    subgraph "1. Event Ingestion"
        A1[External System] -->|Publish| A2[Messaging Topic<br/>domain-users]
        A2 --> A3[Kafka Consumer<br/>Pattern Match]
    end
    
    subgraph "2. Message Processing"
        A3 --> B1[BaseKafkaConsumer<br/>consume]
        B1 --> B2{Validate Payload}
        B2 -->|Invalid| B3[Error handling]
        B2 -->|Valid| B4[PayloadProcessor<br/>Parse JSON]
    end
    
    subgraph "3. Job Initialization"
        B4 --> C1[ImportJobService<br/>startNodesImport]
        C1 --> C2[Create NodesImportJob<br/>Status: PENDING]
        C2 --> C3[(PostgreSQL<br/>nodes_import_job)]
    end
    
    subgraph "4. File Resolution"
        C1 --> D1{Import Type?}
        D1 -->|File-Based| D2[Resolve MultipartFile]
        D1 -->|Reference-Based| D3[Create Nodes from IDs]
        
        D2 --> D4{File Location?}
        D4 -->|Remote URL| D5[RemoteMultipartFile<br/>Download]
        D4 -->|Local Path| D6[FileSystemMultipartFile<br/>Read]
    end
    
    subgraph "5. Streaming Processing"
        D5 --> E1[Open GZIP Stream]
        D6 --> E1
        E1 --> E2[CsvParser.parseInBatches<br/>Stream Processing]
        E2 --> E3[Batch of NodeResponse]
    end
    
    subgraph "6. Batch Processing"
        E3 --> F1[processBatchAsync<br/>Thread Pool]
        D3 --> F1
        F1 --> F2[NodesImportProcessor<br/>processBatch]
        F2 --> F3[Convert to Node Entities]
        F3 --> F4[NodesStorageProcessor<br/>saveNodesSafely]
    end
    
    subgraph "7. Bulk Storage"
        F4 --> G1[Partition Batches]
        G1 --> G2[PostgreSQL COPY Protocol<br/>temp_nodes table]
        G2 --> G3[UPSERT via INSERT ON CONFLICT]
        G3 --> G4[Metadata Batch Insert]
        G4 --> G5[(PostgreSQL<br/>nodes + node_metadata)]
    end
    
    subgraph "8. Status Management"
        F2 --> H1[Increment Processed Count]
        H1 --> H2[NodesImportStatusUpdater]
        H2 --> H3{All Batches Done?}
        H3 -->|Yes & No Failures| H4[completeJob<br/>Status: COMPLETED]
        H3 -->|Has Failures| H5[failJob<br/>Status: FAILED]
        
        H4 --> I1[Build JobExchange Message]
        H5 --> I1
        I1 --> I2[Messaging Producer<br/>domain-job-status-retrieval]
    end
    
    style A2 fill:#4CAF50
    style C3 fill:#2196F3
    style E2 fill:#FF9800
    style G2 fill:#9C27B0
    style I2 fill:#F44336
```

### 4.2 File-Based Import Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant Consumer
    participant JobSvc as Import Job Service
    participant FileSvc as File Import Service
    participant Storage
    participant Parser as CSV Parser
    participant Processor as Import Processor
    participant DB as PostgreSQL
    participant Status as Status Updater
    
    Kafka->>Consumer: NodeExchange message (filePath)
    Consumer->>JobSvc: startNodesImport(payload)
    
    JobSvc->>JobSvc: Validate payload (file-based)
    JobSvc->>DB: Insert NodesImportJob (PENDING)
    DB-->>JobSvc: jobId
    
    JobSvc->>JobSvc: Resolve MultipartFile
    
    alt Remote file
        JobSvc->>Storage: Download file
        Storage-->>JobSvc: RemoteMultipartFile
    else Local file
        JobSvc->>JobSvc: FileSystemMultipartFile
    end
    
    JobSvc->>FileSvc: processNodesImport(jobId, file, exchange)
    FileSvc->>DB: Update job status (PROCESSING)
    
    FileSvc->>FileSvc: Open GZIP stream
    FileSvc->>Parser: parseInBatches(gzipStream, callback)
    
    loop For each batch
        Parser-->>FileSvc: Batch of NodeResponse
        FileSvc->>FileSvc: processBatchAsync (thread pool)
        
        par Parallel Batch Processing
            FileSvc->>Processor: processBatch(jobId, batch, ...)
            Processor->>Processor: Convert NodeResponse → Node
            Processor->>Storage: saveNodesSafely(nodes)
            
            Storage->>Storage: Generate UUIDs if missing
            Storage->>DB: COPY to temp_nodes
            Storage->>DB: INSERT ON CONFLICT (upsert)
            DB-->>Storage: Node mappings
            
            Storage->>DB: Batch insert metadata
            
            Processor->>DB: Increment processed count
            Processor->>Processor: Track success/failed lists
        end
        
        FileSvc->>FileSvc: orTimeout(configurable)
    end
    
    FileSvc->>FileSvc: joinAndClearFutures (final wait)
    FileSvc->>Status: finalizeJob(success, failed, total)
    
    alt All succeeded
        Status->>DB: Update job (COMPLETED)
        Status->>Kafka: Publish success status
    else Some failed
        Status->>DB: Update job (FAILED)
        Status->>Kafka: Publish failure status
    end
```

### 4.3 Reference-Based Import Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant Consumer
    participant JobSvc as Import Job Service
    participant BatchSvc as Batch Import Service
    participant Processor as Import Processor
    participant Storage as Storage Processor
    participant DB as PostgreSQL
    participant Status as Status Updater
    
    Kafka->>Consumer: NodeExchange (referenceIds list)
    Consumer->>JobSvc: startNodesImport(payload)
    
    JobSvc->>JobSvc: Validate payload (reference-based)
    JobSvc->>DB: Insert NodesImportJob (PENDING)
    DB-->>JobSvc: jobId
    
    JobSvc->>JobSvc: createNodesFromReferences(referenceIds)
    JobSvc->>JobSvc: Partition into batches
    
    JobSvc->>BatchSvc: processNodesImport(jobId, referenceIds, groupId, ...)
    BatchSvc->>DB: Update job status (PROCESSING)
    BatchSvc->>DB: Update total nodes count
    
    loop For each batch
        BatchSvc->>BatchSvc: processBatchAsync (thread pool)
        
        par Parallel Processing
            BatchSvc->>Processor: processAndPersist(jobId, groupId, nodes, ...)
            
            Processor->>DB: saveAll(batch)
            DB-->>Processor: Saved entities
            
            Processor->>Processor: Track success list
            Processor->>DB: Increment processed count
        end
        
        BatchSvc->>BatchSvc: orTimeout(configurable)
    end
    
    BatchSvc->>BatchSvc: joinAndClearFutures
    
    alt All batches succeeded
        BatchSvc->>Status: completeJob(jobId, groupId, success, total)
        Status->>DB: Update job (COMPLETED)
        Status->>Kafka: Publish success status
    else Any batch failed
        BatchSvc->>Status: failJob(jobId, groupId, reason, success, failed)
        Status->>DB: Update job (FAILED)
        Status->>Kafka: Publish failure status
    end
```

---

## 5. Kafka Integration

### 5.1 Kafka Configuration Architecture

```mermaid
classDiagram
    class KafkaConfig {
        +kafkaTemplate() KafkaTemplate
        +producerFactory() ProducerFactory
        +consumerFactory() ConsumerFactory
        +kafkaListenerContainerFactory() ConcurrentKafkaListenerContainerFactory
    }
    
    class KafkaListenerConfiguration {
        -ScheduleXPayloadProcessor payloadProcessor
        +nodesImportConfig() KafkaListenerConfig
        +jobStatusConfig() KafkaListenerConfig
    }
    
    class ConcurrentKafkaListenerContainerFactory {
        -ConsumerFactory consumerFactory
        -RecordMessageConverter messageConverter
        -ErrorHandler errorHandler
    }
    
    KafkaConfig --> ConcurrentKafkaListenerContainerFactory : creates
    KafkaListenerConfiguration --> KafkaListenerConfig : provides
```

**Kafka Consumer Configuration**:

Consumers are configured with bounded polling, retries, and error handling routing. All limits are externally configurable.

### 5.2 Topic Patterns & Routing

```mermaid
graph TB
    subgraph "Inbound Topics"
        T1[Pattern: .*-users]
        T2[Pattern: .*-match-suggestions-transfer-job-status-retrieval]
    end
    
    subgraph "Consumer Routing"
        C1[consumeNodesImport]
        C2[consumeJobStatus]
    end
    
    subgraph "Payload Processing"
        P1[processImportedNodesPayload<br/>NodeExchange]
        P2[processNodesImportJobStatusPayload<br/>NodesTransferJobExchange]
    end
    
    subgraph "Outbound Topics"
        O1[Dynamic: domainName-users-transfer-job-status-retrieval]
        O2[Error handling topics]
    end
    
    T1 --> C1
    T2 --> C2
    
    C1 --> P1
    C2 --> P2
    
    P1 -->|Success| O1
    P1 -->|Failure| O2
    
    style T1 fill:#4CAF50
    style C1 fill:#2196F3
    style P1 fill:#FF9800
    style O1 fill:#9C27B0
    style O2 fill:#F44336
```

### 5.3 Error Handling Flow

```mermaid
flowchart TD
    A[Consumer Receives Message] --> B{Payload Valid?}
    
    B -->|Null/Blank| C[Error handling]
    B -->|Valid| D[Submit to Thread Pool]
    
    D --> E[CompletableFuture.runAsync]
    E --> F[payloadProcessor.process.get]
    
    F --> G{Processing Result?}
    
    G -->|Success| H[Processing continuation]
    G -->|Exception| I[Error handling]
    
    E --> L[orTimeout configurable]
    L --> M{Timeout?}
    
    M -->|Yes| N[Error handling]
    
    H --> P[Commit Offset]
    
    subgraph "Framework Level"
        Q[Backoff retries]
        R[Error handling routing]
    end
    
    F -.->|Exception| Q
    
    style C fill:#FFCDD2
    style I fill:#FFCDD2
    style H fill:#C8E6C9
```

---

## 6. File Processing Engine

### 6.1 MultipartFile Abstraction

```mermaid
classDiagram
    class MultipartFile {
        <<interface>>
        +getName() String
        +getOriginalFilename() String
        +getContentType() String
        +isEmpty() boolean
        +getSize() long
        +getBytes() byte[]
        +getInputStream() InputStream
        +transferTo(File) void
        +transferTo(Path) void
    }
    
    class FileSystemMultipartFile {
        -Path filePath
        -String originalFileName
        -String contentType
        +FileSystemMultipartFile(String, String, String)
        +getInputStream() InputStream
        +getSize() long
        +isEmpty() boolean
    }
    
    class RemoteMultipartFile {
        -String bucketName
        -String objectPath
        -String originalFileName
        -String contentType
        +RemoteMultipartFile(String, String, String)
        +getInputStream() InputStream
        +getSize() long
    }
    
    MultipartFile <|.. FileSystemMultipartFile
    MultipartFile <|.. RemoteMultipartFile
```

**File Resolution Logic**:

```mermaid
flowchart TD
    A[NodeExchange.filePath] --> B{Path Type?}
    
    B -->|URL| C[Remote File]
    B -->|Local path| D[Local File]
    
    C --> E[RemoteMultipartFile Constructor]
    E --> F[Parse URL]
    
    D --> M[FileSystemMultipartFile Constructor]
    M --> N[Resolve path]
    N --> O{File Exists?}
    
    O -->|Yes| P{File Readable?}
    O -->|No| Q[Error handling]
    
    P -->|Yes| R[Return FileSystemMultipartFile]
    P -->|No| Q
    
    F --> S[Return RemoteMultipartFile]
    
    style C fill:#BBDEFB
    style D fill:#C8E6C9
    style Q fill:#FFCDD2
    style R fill:#4CAF50
    style S fill:#4CAF50
```

### 6.2 CSV Streaming Processing

```mermaid
sequenceDiagram
    participant Svc as NodesImportService
    participant File as MultipartFile
    participant GZIP as GZIPInputStream
    participant Parser as CsvParser
    participant Factory as ResponseFactory
    participant Callback as Batch Callback
    participant Executor as Thread Pool
    
    Svc->>File: getInputStream()
    File-->>Svc: InputStream
    
    Svc->>GZIP: new GZIPInputStream(inputStream)
    GZIP-->>Svc: Decompressed stream
    
    Svc->>Parser: parseInBatches(gzipStream, factory, callback)
    
    loop Stream not exhausted
        Parser->>Parser: Read rows
        Parser->>Factory: createFromRow(row)
        Factory-->>Parser: List<NodeResponse>
        
        Parser->>Callback: accept(batch)
        
        Callback->>Svc: processBatchAsync(batch)
        Svc->>Executor: Submit batch processing
        
        alt Concurrent limit
            Svc->>Svc: Manage concurrent batches
        end
    end
    
    Parser->>Svc: Parsing complete
    Svc->>Svc: joinAndClearFutures (final wait)
```

**Batch Processing with Timeout**:

```mermaid
flowchart LR
    A[Batch of Nodes] --> B[Calculate Timeout<br/>Configurable per batch]
    
    B --> C[Timeout application]
    
    C --> D[CompletableFuture.orTimeout<br/>configurable timeout]
    
    D --> E{Result?}
    E -->|Success| F[Return]
    E -->|Timeout| G[Error handling<br/>Add to failed list]
    
    style A fill:#4CAF50
    style C fill:#2196F3
    style D fill:#FF9800
    style G fill:#FFCDD2
```

### 6.3 Parallel Batch Management

```mermaid
stateDiagram-v2
    [*] --> Idle: System Ready
    
    Idle --> Reading: Start CSV Stream
    
    Reading --> Buffering: Accumulate rows
    Buffering --> BatchReady: Buffer ready
    
    BatchReady --> CheckParallel: Create CompletableFuture
    
    CheckParallel --> Submit: Slots available
    CheckParallel --> Wait: Limit reached
    
    Wait --> Cleanup: Remove completed
    Cleanup --> Submit
    
    Submit --> Processing: Execute in thread pool
    
    Processing --> Success: Batch completes
    Processing --> Timeout: Exceeds timeout
    Processing --> Error: Exception thrown
    
    Success --> Reading: More rows?
    Timeout --> MarkFailed: Add to failed list
    Error --> MarkFailed
    
    MarkFailed --> Reading: More rows?
    
    Reading --> AllRead: Stream exhausted
    AllRead --> JoinAll: joinAndClearFutures
    
    JoinAll --> Finalize: All complete
    
    Finalize --> [*]: Job complete
```

---

## 7. Storage Architecture

### 7.1 PostgreSQL COPY Protocol Implementation

```mermaid
classDiagram
    class NodesStorageProcessor {
        -JdbcTemplate jdbcTemplate
        -NodeMetadataBatchWriter metadataBatchWriter
        -TransactionTemplate transactionTemplate
        -RetryTemplate retryTemplate
        +saveNodesSafely(List~Node~) CompletableFuture~Void~
        -upsertNodes(List~Node~) ConcurrentHashMap
        -upsertBatch(List~Node~, Map) void
        -buildCsvRows(List~Node~) String
        -executeCopy(String) List~Map~
        -insertMetadata(List~Node~, Collection~UUID~) void
    }
    
    class CopyManager {
        <<PostgreSQL>>
        +copyIn(String, InputStream) void
    }
    
    class NodeMetadataBatchWriter {
        +batchInsertMetadata(Map~UUID, Map~String,String~~) void
    }
    
    NodesStorageProcessor --> CopyManager : uses
    NodesStorageProcessor --> NodeMetadataBatchWriter : delegates
```

**COPY Protocol Flow**:

```mermaid
sequenceDiagram
    autonumber
    participant Processor as NodesStorageProcessor
    participant Txn as TransactionTemplate
    participant DS as DataSource
    participant Conn as Connection
    participant Copy as CopyManager
    participant DB as PostgreSQL
    
    Processor->>Processor: Partition nodes into batches
    
    loop For each batch
        Processor->>Txn: execute(status -> ...)
        Txn->>DS: getConnection()
        DS-->>Txn: Connection
        
        Txn->>Conn: setAutoCommit(false)
        
        Txn->>Conn: createStatement()
        Txn->>DB: CREATE TEMP TABLE temp_nodes (...)
        
        Txn->>Processor: buildCsvRows(batch)
        Processor-->>Txn: TSV string data
        
        Txn->>Txn: new ByteArrayInputStream(csvData)
        Txn->>Copy: new CopyManager(conn.unwrap(BaseConnection))
        
        Txn->>Copy: copyIn("COPY temp_nodes FROM STDIN WITH CSV DELIMITER '\t'", stream)
        Copy->>DB: Stream TSV data
        DB-->>Copy: Rows copied
        
        Txn->>Conn: prepareStatement(UPSERT_SQL)
        Txn->>DB: INSERT INTO nodes SELECT * FROM temp_nodes<br/>ON CONFLICT DO UPDATE
        
        DB-->>Txn: ResultSet with node mappings
        
        Txn->>Processor: Store mappings
        
        Txn->>Conn: commit()
    end
    
    Processor->>Processor: Collect valid nodeIds
    Processor->>Processor: insertMetadata(nodes, validNodeIds)
```

**TSV Format (Tab-Separated Values)**:

```
UUID\tReferenceId\tGroupId\tType\tDomainId\tCreatedAt
...
```

**UPSERT SQL**:

```sql
INSERT INTO public.nodes (id, reference_id, group_id, type, domain_id, created_at)
SELECT id, reference_id, group_id, type, domain_id, created_at
FROM temp_nodes
ON CONFLICT (group_id, domain_id, reference_id)
DO UPDATE SET
    type = EXCLUDED.type,
    created_at = EXCLUDED.created_at
RETURNING id, reference_id, group_id;
```

### 7.2 Metadata Batch Insert

```mermaid
flowchart TD
    A[Nodes with Metadata] --> B[Filter: validNodeIds only]
    B --> C[Extract metadata maps]
    
    C --> D{Metadata Empty?}
    D -->|Yes| E[Skip, Return]
    D -->|No| F[NodeMetadataBatchWriter<br/>batchInsertMetadata]
    
    F --> G[Flatten to List~NodeMetadata~]
    G --> H[Partition batches]
    
    H --> I[For each batch]
    I --> J[CREATE TEMP TABLE temp_node_metadata]
    J --> K[COPY to temp table]
    K --> L[INSERT ON CONFLICT DO UPDATE]
    L --> M[COMMIT]
    
    M --> N[Return]
    
    style A fill:#4CAF50
    style F fill:#2196F3
    style K fill:#FF9800
```

### 7.3 Transaction & Retry Strategy

```mermaid
graph TB
    subgraph "Retry Template"
        A[Attempts configurable]
        B[Backoff configurable]
        C[Retry On: DataAccessException]
    end
    
    subgraph "Transaction Template"
        D[Propagation: REQUIRED]
        E[Isolation: READ_COMMITTED]
    end
    
    subgraph "Execution Flow"
        G[retryTemplate.execute]
        H[transactionTemplate.execute]
        I[Database Operation]
    end
    
    A --> G
    B --> G
    C --> G
    
    D --> H
    E --> H
    
    G --> H
    H --> I
    
    I --> J{Result?}
    J -->|Success| K[Commit & Return]
    J -->|Exception| L{Retry available?}
    
    L -->|Yes| M[Backoff & Retry]
    L -->|No| N[Rollback & Throw]
    
    M --> G
    
    style K fill:#C8E6C9
    style N fill:#FFCDD2
```

---

## 8. Error Handling & Resilience

### 8.1 Error Handling Hierarchy

```mermaid
graph TB
    subgraph "Layer 1: Kafka Consumer"
        L1A[Message Validation]
        L1B[Error Routing]
        L1C[Async Timeout]
    end
    
    subgraph "Layer 2: Framework"
        L2A[ErrorHandler]
        L2B[Backoff]
        L2C[Recovery]
    end
    
    subgraph "Layer 3: Application Processing"
        L3A[Payload Parsing]
        L3B[Validation]
        L3C[File Resolution]
    end
    
    subgraph "Layer 4: Batch Processing"
        L4A[Batch Timeout]
        L4B[Exception Tracking]
        L4C[Join Timeout]
    end
    
    subgraph "Layer 5: Storage"
        L5A[RetryTemplate]
        L5B[TransactionTemplate<br/>Rollback on error]
        L5C[COPY Failure Recovery]
    end
    
    L1A --> L2A
    L1B --> L2C
    L2A --> L3A
    L3A --> L4A
    L4A --> L5A
    
    style L1A fill:#E8F5E9
    style L2A fill:#E3F2FD
    style L3A fill:#FFF9C4
    style L4A fill:#F3E5F5
    style L5A fill:#FFEBEE
```

### 8.2 Failure Scenarios & Recovery

```mermaid
flowchart TD
    A[Processing Started] --> B{Failure Type?}
    
    B -->|Kafka Consumer Failure| C1[Message Validation Failed]
    B -->|File Download Failure| C2[Connection Error]
    B -->|Parse Failure| C3[CSV Format Invalid]
    B -->|Batch Timeout| C4[Processing Exceeds Timeout]
    B -->|Database Failure| C5[COPY Error]
    B -->|Join Timeout| C6[Batches Not Completing]
    
    C1 --> D1[Error handling]
    C2 --> D2[Retry with backoff]
    C3 --> D3[Skip row, Continue]
    C4 --> D4[Cancel future<br/>Add to failed list]
    C5 --> D5[Rollback transaction<br/>Retry]
    C6 --> D6[Cancel futures<br/>Mark job FAILED]
    
    D2 --> E2[Error handling if exhausted]
    D5 --> E4{Retry exhausted?}
    
    E4 -->|Yes| E3[Mark job FAILED]
    E4 -->|No| F[Retry]
    
    E3 --> G[Publish FAILED status]
    D6 --> G
    
    G --> H[Update job status in DB]
    
    style C1 fill:#FFCDD2
    style C2 fill:#FFCDD2
    style C5 fill:#FFCDD2
    style E3 fill:#FFF9C4
```

### 8.3 Status Tracking State Machine

```mermaid
stateDiagram-v2
    [*] --> PENDING: Job Created

    PENDING --> PROCESSING: Start Import

    PROCESSING --> COMPLETED: All batches succeeded
    PROCESSING --> FAILED: Failure detected

    FAILED --> [*]: Publish failure status
    COMPLETED --> [*]: Publish success status

    note right of PENDING
        Initial state
        Job ID assigned
    end note

    note right of PROCESSING
        Batches in progress
        Processed count increasing
    end note

    note right of COMPLETED
        All nodes processed
        Success
    end note

    note right of FAILED
        Error occurred
        Failure reason stored
    end note

```

**Status Update Events**:

| Event | From Status | To Status | Database Update | Messaging Message |
|-------|------------|-----------|-----------------|-------------------|
| **Job Created** | N/A | PENDING | INSERT job row | No |
| **Processing Started** | PENDING | PROCESSING | UPDATE status | No |
| **Batch Completed** | PROCESSING | PROCESSING | INCREMENT processed | No |
| **All Success** | PROCESSING | COMPLETED | UPDATE status | Yes (success) |
| **Failure** | PROCESSING | FAILED | UPDATE status, SET reason | Yes (failed) |

---

## Appendix A: Database Schema

### A.1 Core Tables

```sql
-- Nodes Import Job Table
CREATE TABLE public.nodes_import_job (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,  -- PENDING, PROCESSING, COMPLETED, FAILED
    total_nodes INT DEFAULT 0,
    processed_nodes INT DEFAULT 0,
    failure_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_import_job_status ON nodes_import_job(status);
CREATE INDEX idx_import_job_group ON nodes_import_job(group_id, domain_id);

-- Nodes Table
CREATE TABLE public.nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reference_id VARCHAR(255) NOT NULL,
    group_id UUID NOT NULL,
    type VARCHAR(50) NOT NULL,
    domain_id UUID NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP,
    CONSTRAINT uq_node_reference UNIQUE (group_id, domain_id, reference_id)
);

CREATE INDEX idx_nodes_group_domain ON nodes(group_id, domain_id);
CREATE INDEX idx_nodes_processed ON nodes(group_id, domain_id, processed);

-- Node Metadata Table
CREATE TABLE public.node_metadata (
    id BIGSERIAL PRIMARY KEY,
    node_id UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    metadata_key VARCHAR(255) NOT NULL,
    metadata_value TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_node_metadata UNIQUE (node_id, metadata_key)
);

CREATE INDEX idx_metadata_node ON node_metadata(node_id);
CREATE INDEX idx_metadata_key ON node_metadata(metadata_key);
```