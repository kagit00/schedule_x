# Match Transfer System - Low-Level Design Document


---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Design](#2-architecture-design)
3. [Component Design](#3-component-design)
4. [Data Flow Architecture](#4-data-flow-architecture)
5. [Concurrency & Threading Model](#5-concurrency--threading-model)
6. [Streaming Architecture](#6-streaming-architecture)
7. [Export & File Processing](#7-export--file-processing)
8. [Kafka Integration](#8-kafka-integration)
9. [Error Handling & Resilience](#9-error-handling--resilience)
10. [Performance Optimization](#10-performance-optimization)
11. [Monitoring & Observability](#11-monitoring--observability)

---

## 1. System Overview

### 1.1 Purpose

The **Match Transfer System** is a scheduled batch export platform that streams match results (potential and perfect matches) from PostgreSQL, exports them to Parquet files, and publishes file references to Kafka topics. It supports multi-tenant processing with concurrent group execution, memory-efficient streaming, and comprehensive error handling.

### 1.2 Key Capabilities

```mermaid
mindmap
  root((Match Transfer<br/>System))
    Scheduling
      Cron-based Execution
      Multi-Domain Processing
      Concurrent Group Handling
    Streaming
      Potential Matches
      Perfect Matches
      Memory-Efficient
      Batch Processing
    Export
      Parquet Format
      Compression
      Schema Management
      Semaphore Control
    Publishing
      Kafka Topics
      File References
      DLQ Support
      Async Callbacks
    Resilience
      Circuit Breaker
      Retry Mechanisms
      Fallback Handling
      Error Tracking
```

### 1.3 System Metrics

| Metric | Target | Current Capacity |
|--------|--------|------------------|
| **Throughput** | 100K matches/sec | 50K matches/sec |
| **Export Time** | <5 min for 10M matches | ~3 min |
| **Memory Footprint** | <2GB per group | ~1.5GB |
| **Concurrent Groups** | 50 simultaneous | 20 tested |
| **File Compression Ratio** | 10:1 (Parquet SNAPPY) | 8:1 |
| **Kafka Publishing Rate** | 100 msg/sec | 80 msg/sec |

---

## 2. Architecture Design

### 2.1 System Context Diagram

```mermaid
C4Context
    title System Context - Match Transfer System
    
    Person(scheduler, "Cron Scheduler", "Triggers periodic exports")
    Person(ops, "Operations Team", "Monitors export jobs")
    
    System_Boundary(transfer_boundary, "Match Transfer System") {
        System(transfersys, "Match Transfer Engine", "Exports matches to Parquet files")
    }
    
    SystemDb_Ext(postgres, "PostgreSQL", "Potential & Perfect matches storage")
    System_Ext(kafka, "Kafka Cluster", "File reference events")
    System_Ext(filesystem, "File System / MinIO", "Parquet file storage")
    System_Ext(monitoring, "Prometheus + Grafana", "Observability")
    System_Ext(downstream, "Downstream Consumers", "Process exported files")
    
    Rel(scheduler, transfersys, "Triggers daily", "Cron")
    Rel(transfersys, postgres, "Streams matches", "JDBC ResultSet")
    Rel(transfersys, filesystem, "Writes Parquet files", "File I/O")
    Rel(transfersys, kafka, "Publishes file refs", "Kafka Producer")
    Rel(transfersys, monitoring, "Exports metrics", "HTTP")
    Rel(kafka, downstream, "Notifies", "Kafka Consumer")
    Rel(downstream, filesystem, "Downloads files", "File I/O")
    Rel(ops, monitoring, "Views dashboards", "HTTPS")
    
    UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="1")
```

### 2.2 Logical Architecture

```mermaid
graph TB
    subgraph "Scheduling Layer"
        A1[MatchesTransferScheduler<br/>Cron Trigger]
        A2[Group Task Distribution]
    end
    
    subgraph "Orchestration Layer"
        B1[MatchTransferService<br/>Delegation]
        B2[MatchTransferProcessor<br/>Core Logic]
        B3[Circuit Breaker<br/>Resilience4j]
    end
    
    subgraph "Streaming Layer"
        C1[PotentialMatchStreamingService<br/>JDBC Streaming]
        C2[PerfectMatchStreamingService<br/>JDBC Streaming]
        C3[Blocking Queue<br/>Producer-Consumer]
    end
    
    subgraph "Export Layer"
        D1[ExportService<br/>Parquet Writer]
        D2[Schema Manager]
        D3[Field Extractors]
        D4[Semaphore Control]
    end
    
    subgraph "Publishing Layer"
        E1[ScheduleXProducer<br/>Kafka Publisher]
        E2[DLQ Handler]
        E3[Async Callbacks]
    end
    
    subgraph "Infrastructure"
        F1[(PostgreSQL<br/>Match Tables)]
        F2[(File System<br/>Parquet Files)]
        F3[Kafka<br/>Topics]
        F4[Metrics<br/>Prometheus]
    end
    
    A1 --> B1
    A2 --> B1
    B1 --> B2
    B2 --> B3
    
    B2 --> C1
    B2 --> C2
    C1 --> C3
    C2 --> C3
    
    C3 --> D1
    D1 --> D2
    D2 --> D3
    D1 --> D4
    
    D1 --> E1
    E1 --> E2
    E1 --> E3
    
    C1 --> F1
    C2 --> F1
    D1 --> F2
    E1 --> F3
    B2 --> F4
    
    style A1 fill:#4CAF50
    style B2 fill:#2196F3
    style C3 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#F44336
    style F1 fill:#607D8B
```

### 2.3 Component Architecture

```mermaid
C4Container
    title Container Diagram - Match Transfer System
    
    Container_Boundary(app, "Transfer Application") {
        Container(scheduler, "Scheduler", "Spring @Scheduled", "Triggers periodic exports")
        Container(processor, "Transfer Processor", "Java Service", "Orchestrates export workflow")
        Container(potentialstream, "Potential Stream Service", "Java Component", "Streams potential matches")
        Container(perfectstream, "Perfect Stream Service", "Java Component", "Streams perfect matches")
        Container(exporter, "Export Service", "Java Service", "Writes Parquet files")
        Container(producer, "Kafka Producer", "Spring Kafka", "Publishes file references")
    }
    
    ContainerDb(postgres, "PostgreSQL", "Relational DB", "potential_matches, perfect_matches")
    ContainerDb(filesystem, "File System", "Storage", "Parquet files")
    Container(kafka, "Kafka", "Message Broker", "Export topics")
    
    Rel(scheduler, processor, "Triggers", "Async")
    Rel(processor, potentialstream, "Streams", "Consumer callback")
    Rel(processor, perfectstream, "Streams", "Consumer callback")
    Rel(potentialstream, postgres, "SELECT streaming", "JDBC")
    Rel(perfectstream, postgres, "SELECT streaming", "JDBC")
    Rel(processor, exporter, "Supplies stream", "Supplier<Stream>")
    Rel(exporter, filesystem, "Writes", "Parquet API")
    Rel(processor, producer, "Publishes", "Kafka API")
    Rel(producer, kafka, "Send", "Kafka Protocol")
    
    UpdateLayoutConfig($c4ShapeInRow="3")
```

---

## 3. Component Design

### 3.1 Scheduler Component

```mermaid
classDiagram
    class MatchesTransferScheduler {
        -MatchTransferService matchTransferService
        -MatchingGroupRepository matchingGroupRepository
        -DomainService domainService
        -Executor matchTransferGroupExecutor
        -MeterRegistry meterRegistry
        +scheduledMatchesTransferJob() void
        -monitorExecutorMetrics() void
    }
    
    class MatchTransferService {
        -MatchTransferProcessor matchTransferProcessor
        +processGroup(UUID, Domain) void
    }
    
    class Domain {
        +UUID id
        +String name
        +boolean active
    }
    
    class ThreadPoolTaskExecutor {
        <<Spring Framework>>
        +getActiveCount() int
        +getQueueSize() int
        +getThreadPoolExecutor() ThreadPoolExecutor
    }
    
    MatchesTransferScheduler --> MatchTransferService
    MatchesTransferScheduler --> Domain : processes
    MatchesTransferScheduler ..> ThreadPoolTaskExecutor : monitors
```

**Scheduler Flow**:

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Spring Scheduler
    participant Sched as MatchesTransferScheduler
    participant Metrics as MeterRegistry
    participant Executor as Group Executor
    participant Service as MatchTransferService
    
    Cron->>Sched: Trigger (cron schedule)
    Sched->>Sched: Get Active Domains
    
    loop For each domain
        Sched->>Sched: Get Group IDs
        
        Sched->>Metrics: Monitor executor state
        Note over Sched,Metrics: Active threads, queue size
        
        loop For each group
            Sched->>Executor: Submit async task
            
            par Parallel Group Processing
                Executor->>Service: processGroup(groupId, domain)
                
                alt Processing succeeds
                    Service-->>Executor: Success
                    Executor->>Metrics: Record success duration
                else Processing fails
                    Service-->>Executor: Exception
                    Executor->>Metrics: Increment failure counter
                end
            end
        end
    end
```

### 3.2 Transfer Processor Component

```mermaid
classDiagram
    class MatchTransferProcessor {
        -PotentialMatchStreamingService potentialMatchStreamingService
        -PerfectMatchStreamingService perfectMatchStreamingService
        -ExportService exportService
        -ScheduleXProducer scheduleXProducer
        -Executor matchTransferExecutor
        -int batchSize
        +processMatchTransfer(UUID, Domain) CompletableFuture~Void~
        -exportAndSend(UUID, Domain, Supplier) CompletableFuture~Void~
        -exportAndSendSync(UUID, Domain, Supplier) void
        +processMatchTransferFallback(UUID, Domain, Throwable) CompletableFuture~Void~
    }
    
    class CircuitBreaker {
        <<annotation>>
        +name: String
        +fallbackMethod: String
    }
    
    class Retryable {
        <<annotation>>
        +value: Class[]
        +maxAttempts: int
        +backoff: Backoff
    }
    
    class BlockingQueue~List~MatchTransfer~~ {
        <<Java Concurrency>>
        +put(List) void
        +poll(long, TimeUnit) List
    }
    
    MatchTransferProcessor ..> CircuitBreaker
    MatchTransferProcessor ..> Retryable
    MatchTransferProcessor --> BlockingQueue : uses
```

**Producer-Consumer Pattern**:

```mermaid
flowchart TB
    A[Start processMatchTransfer] --> B[Create BlockingQueue<br/>Capacity: 100]
    
    B --> C[Setup Atomic Counters<br/>recordCount, done]
    
    C --> D[Define potentialConsumer]
    C --> E[Define perfectConsumer]
    
    D --> F[Launch Potential Stream<br/>CompletableFuture.runAsync]
    E --> G[Launch Perfect Stream<br/>CompletableFuture.runAsync]
    
    F --> H[Stream from PostgreSQL<br/>Batch 100K records]
    G --> I[Stream from PostgreSQL<br/>Batch 100K records]
    
    H --> J[Convert to MatchTransfer]
    I --> J
    
    J --> K[queue.put<br/>Blocking if full]
    
    K --> L{Both Streams Done?}
    L -->|No| H
    L -->|Yes| M[Set done=true]
    
    B --> N[Create matchStreamSupplier<br/>Stream.generate]
    
    N --> O[queue.poll 300ms timeout]
    O --> P{done && empty?}
    P -->|No| Q[Yield batch]
    P -->|Yes| R[End stream]
    
    Q --> O
    
    M --> S[exportAndSend<br/>Consume stream]
    R --> S
    
    S --> T[Write Parquet File]
    T --> U[Publish to Kafka]
    
    style B fill:#4CAF50
    style K fill:#FF9800
    style S fill:#2196F3
    style T fill:#9C27B0
```

---

## 4. Data Flow Architecture

### 4.1 End-to-End Data Flow

```mermaid
flowchart TB
    subgraph "1. Initialization"
        A1[Cron Trigger<br/>scheduled time]
        A2[Get Active Domains]
        A3[Get Groups per Domain]
    end
    
    subgraph "2. Concurrent Group Processing"
        B1[For each group<br/>Submit to Executor]
        B2[Circuit Breaker Check]
        B3[Create BlockingQueue<br/>Capacity 100]
    end
    
    subgraph "3. Parallel Streaming"
        C1[Potential Stream Thread]
        C2[Perfect Stream Thread]
        
        C1 --> C3[SELECT * FROM potential_matches<br/>WHERE group_id=? AND domain_id=?]
        C2 --> C4[SELECT * FROM perfect_matches<br/>WHERE group_id=? AND domain_id=?]
        
        C3 --> C5[ResultSet.next<br/>Batch 100K]
        C4 --> C6[ResultSet.next<br/>Batch 100K]
        
        C5 --> C7[Convert to Entity]
        C6 --> C7
        
        C7 --> C8[Transform to MatchTransfer]
        C8 --> C9[queue.put]
    end
    
    subgraph "4. Stream Generation"
        D1[matchStreamSupplier]
        D2[Stream.generate<br/>poll from queue]
        D3[takeWhile !done && !empty]
        D4[flatMap batches]
    end
    
    subgraph "5. Export Processing"
        E1[ParquetWriter.write]
        E2[Schema Validation]
        E3[Field Extraction]
        E4[Compression SNAPPY]
        E5[File: /basedir/domainId/groupId/file.parquet]
    end
    
    subgraph "6. Kafka Publishing"
        F1[Build MatchSuggestionsExchange]
        F2[Topic: domain-matches-suggestions]
        F3[Key: domainId-groupId]
        F4[KafkaTemplate.send]
        F5{Send Success?}
        F5 -->|No| F6[Send to DLQ]
        F5 -->|Yes| F7[Record Metrics]
    end
    
    A1 --> A2
    A2 --> A3
    A3 --> B1
    B1 --> B2
    B2 --> B3
    
    B3 --> C1
    B3 --> C2
    
    C9 --> D1
    D1 --> D2
    D2 --> D3
    D3 --> D4
    
    D4 --> E1
    E1 --> E2
    E2 --> E3
    E3 --> E4
    E4 --> E5
    
    E5 --> F1
    F1 --> F2
    F2 --> F3
    F3 --> F4
    F4 --> F5
    
    style A1 fill:#E8F5E9
    style C9 fill:#FFF9C4
    style E1 fill:#E1F5FE
    style F4 fill:#FFEBEE
```

### 4.2 Detailed Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant Sched as Scheduler
    participant Proc as MatchTransferProcessor
    participant PotentialSvc as PotentialStreamService
    participant PerfectSvc as PerfectStreamService
    participant Queue as BlockingQueue
    participant DB as PostgreSQL
    participant Export as ExportService
    participant Kafka as ScheduleXProducer
    
    Sched->>Proc: processMatchTransfer(groupId, domain)
    
    Proc->>Proc: Create BlockingQueue (capacity 100)
    Proc->>Proc: Setup atomic counters (recordCount, done)
    
    par Parallel Streaming
        Proc->>PotentialSvc: streamAllMatches(groupId, domainId, consumer, 100K)
        PotentialSvc->>DB: SELECT * FROM potential_matches
        DB-->>PotentialSvc: ResultSet stream
        
        loop While ResultSet.next()
            PotentialSvc->>PotentialSvc: Create PotentialMatchEntity
            PotentialSvc->>PotentialSvc: Transform to MatchTransfer
            PotentialSvc->>PotentialSvc: Accumulate to batch (100K)
            
            alt Batch full
                PotentialSvc->>Queue: put(batch) - blocks if full
                Queue-->>PotentialSvc: Accepted
            end
        end
        
        PotentialSvc->>Queue: put(final batch)
    and
        Proc->>PerfectSvc: streamAllMatches(groupId, domainId, consumer, 100K)
        PerfectSvc->>DB: SELECT * FROM perfect_matches
        DB-->>PerfectSvc: ResultSet stream
        
        loop While ResultSet.next()
            PerfectSvc->>PerfectSvc: Create PerfectMatchEntity
            PerfectSvc->>PerfectSvc: Transform to MatchTransfer
            PerfectSvc->>PerfectSvc: Accumulate to batch (100K)
            
            alt Batch full
                PerfectSvc->>Queue: put(batch) - blocks if full
                Queue-->>PerfectSvc: Accepted
            end
        end
        
        PerfectSvc->>Queue: put(final batch)
    end
    
    Note over PotentialSvc,PerfectSvc: Both complete, set done=true
    
    Proc->>Proc: Create matchStreamSupplier
    Proc->>Export: exportMatches(streamSupplier, groupId, domainId)
    
    loop Stream not exhausted
        Export->>Queue: poll(300ms)
        alt Batch available
            Queue-->>Export: List<MatchTransfer>
            Export->>Export: Convert to Parquet records
            Export->>Export: Write to file
        else Timeout
            Queue-->>Export: null
            Export->>Export: Check if done
        end
    end
    
    Export->>Export: Close Parquet file
    Export-->>Proc: ExportedFile
    
    Proc->>Kafka: sendMessage(topic, key, payload)
    Kafka->>Kafka: KafkaTemplate.send
    
    alt Send success
        Kafka-->>Proc: Success callback
    else Send failure
        Kafka->>Kafka: sendToDlq(key, payload)
    end
```

---

## 5. Concurrency & Threading Model

### 5.1 Thread Pool Architecture

```mermaid
graph TB
    subgraph "Scheduler Thread"
        ST[Spring @Scheduled Thread<br/>Single threaded]
    end
    
    subgraph "Group Executor Pool"
        GE[matchTransferGroupExecutor<br/>ThreadPoolTaskExecutor]
        GE1[group-exec-1]
        GE2[group-exec-2]
        GEN[group-exec-N]
        GE --> GE1
        GE --> GE2
        GE --> GEN
    end
    
    subgraph "Batch Executor Pool"
        BE[matchTransferExecutor<br/>ThreadPoolTaskExecutor]
        BE1[batch-exec-1<br/>Potential Stream]
        BE2[batch-exec-2<br/>Perfect Stream]
        BE3[batch-exec-3<br/>Export]
        BEN[batch-exec-N]
        BE --> BE1
        BE --> BE2
        BE --> BE3
        BE --> BEN
    end
    
    subgraph "Kafka Callback Pool"
        KE[kafkaCallbackExecutor<br/>FixedThreadPool]
        KE1[kafka-cb-1]
        KE2[kafka-cb-2]
        KEN[kafka-cb-N]
        KE --> KE1
        KE --> KE2
        KE --> KEN
    end
    
    ST -->|Submit group tasks| GE
    GE1 -->|Submit streaming| BE
    GE2 -->|Submit streaming| BE
    BE1 -->|DB streaming| DB[(PostgreSQL)]
    BE2 -->|DB streaming| DB
    BE3 -->|Write file| FS[(File System)]
    BE3 -->|Send Kafka| KE
    
    style ST fill:#4CAF50
    style GE fill:#2196F3
    style BE fill:#FF9800
    style KE fill:#9C27B
```