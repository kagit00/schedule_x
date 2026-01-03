# Match Transfer System - Low-Level Design Document



---

This document describes the low-level design and reference implementation of a batch-oriented match export pipeline, focusing on correctness, data flow, and concurrency patterns rather than production deployment or operational tuning.

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Design](#2-architecture-design)
3. [Component Design](#3-component-design)
4. [Data Flow Architecture](#4-data-flow-architecture)

---

## 1. System Overview

### 1.1 Purpose

The **Match Transfer System** is a scheduled batch export component that streams match results (potential and perfect matches) from PostgreSQL, exports them to Parquet files, and publishes file references to messaging topics. It supports group-isolated batch processing with fault handling.

### 1.2 Key Capabilities

```mermaid
graph TB
    ROOT[Match Transfer System]

    ROOT --> S[Scheduling]
    ROOT --> ST[Streaming]
    ROOT --> E[Export]
    ROOT --> P[Publishing]
    ROOT --> R[Resilience]

    S --> S1[Cron Based Execution]
    S --> S2[Multi Domain Processing]
    S --> S3[Concurrent Group Handling]

    ST --> ST1[Potential Matches Stream]
    ST --> ST2[Perfect Matches Stream]
    ST --> ST3[Memory Efficient Processing]
    ST --> ST4[Batch Processing]

    E --> E1[Parquet Format]
    E --> E2[Compression]
    E --> E3[Schema Management]
    E --> E4[Concurrency Control]

    P --> P1[Messaging Topics]
    P --> P2[File References]
    P --> P3[Error Handling]
    P --> P4[Async Operations]

    R --> R1[Circuit Breaker]
    R --> R2[Retry Mechanisms]
    R --> R3[Fallback Handling]
    R --> R4[Error Tracking]

```

---

## 2. Architecture Design

### 2.1 Logical Architecture

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
        D4[Concurrency Control]
    end
    
    subgraph "Publishing Layer"
        E1[ScheduleXProducer<br/>Messaging Publisher]
        E2[Error Handler]
        E3[Async Operations]
    end
    
    subgraph "Infrastructure"
        F1[(PostgreSQL<br/>Match Tables)]
        F2[(File System<br/>Parquet Files)]
        F3[Messaging System<br/>Topics]
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
    
    style A1 fill:#4CAF50
    style B2 fill:#2196F3
    style C3 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#F44336
    style F1 fill:#607D8B
```

### 2.2 Component Architecture

```mermaid
C4Container
    title Container Diagram - Match Transfer System
    
    Container_Boundary(app, "Transfer Application") {
        Container(scheduler, "Scheduler", "Spring @Scheduled", "Triggers periodic exports")
        Container(processor, "Transfer Processor", "Java Service", "Orchestrates export workflow")
        Container(potentialstream, "Potential Stream Service", "Java Component", "Streams potential matches")
        Container(perfectstream, "Perfect Stream Service", "Java Component", "Streams perfect matches")
        Container(exporter, "Export Service", "Java Service", "Writes Parquet files")
        Container(producer, "Messaging Producer", "Messaging Client", "Publishes file references")
    }
    
    ContainerDb(postgres, "PostgreSQL", "Relational DB", "potential_matches, perfect_matches")
    ContainerDb(filesystem, "File System", "Storage", "Parquet files")
    Container(messaging, "Messaging System", "Message Broker", "Export topics")
    
    Rel(scheduler, processor, "Triggers", "Async")
    Rel(processor, potentialstream, "Streams", "Consumer callback")
    Rel(processor, perfectstream, "Streams", "Consumer callback")
    Rel(potentialstream, postgres, "SELECT streaming", "JDBC")
    Rel(perfectstream, postgres, "SELECT streaming", "JDBC")
    Rel(processor, exporter, "Supplies stream", "Supplier<Stream>")
    Rel(exporter, filesystem, "Writes", "Parquet API")
    Rel(processor, producer, "Publishes", "Messaging API")
    Rel(producer, messaging, "Send", "Protocol")
    
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
        +scheduledMatchesTransferJob() void
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
    
    MatchesTransferScheduler --> MatchTransferService
    MatchesTransferScheduler --> Domain : processes
```

**Scheduler Flow**:

```mermaid
sequenceDiagram
    autonumber
    participant Cron as Spring Scheduler
    participant Sched as MatchesTransferScheduler
    participant Executor as Group Executor
    participant Service as MatchTransferService
    
    Cron->>Sched: Trigger (cron schedule)
    Sched->>Sched: Get Active Domains
    
    loop For each domain
        Sched->>Sched: Get Group IDs
        
        loop For each group
            Sched->>Executor: Submit async task
            
            par Parallel Group Processing
                Executor->>Service: processGroup(groupId, domain)
                
                alt Processing succeeds
                    Service-->>Executor: Success
                else Processing fails
                    Service-->>Executor: Exception
                end
            end
        end
    end
```

### 3.2 Transfer Processor Component

```mermaid
classDiagram
    class MatchTransferProcessor {
        -potentialMatchStreamingService
        -perfectMatchStreamingService
        -exportService
        -scheduleXProducer
        -matchTransferExecutor
        +processMatchTransfer(id, domain)
        -exportAndSend(id, domain, supplier)
        -exportAndSendSync(id, domain, supplier)
        +processMatchTransferFallback(id, domain, error)
    }

    class CircuitBreaker {
        <<annotation>>
        name : String
        fallbackMethod : String
    }

    class Retryable {
        <<annotation>>
        maxAttempts : int
        backoff : Backoff
    }

    class MatchTransferQueue {
        <<concurrent>>
        put(batch)
        poll(timeout)
    }

    MatchTransferProcessor ..> CircuitBreaker : protected by
    MatchTransferProcessor ..> Retryable : retried by
    MatchTransferProcessor --> MatchTransferQueue : uses

```

**Producer-Consumer Pattern**:

```mermaid
flowchart TB
    A[Start processMatchTransfer] --> B[Create BlockingQueue]
    
    B --> C[Setup Atomic Counters<br/>recordCount, done]
    
    C --> D[Define potentialConsumer]
    C --> E[Define perfectConsumer]
    
    D --> F[Launch Potential Stream<br/>CompletableFuture.runAsync]
    E --> G[Launch Perfect Stream<br/>CompletableFuture.runAsync]
    
    F --> H[Stream from PostgreSQL<br/>Configurable batches]
    G --> I[Stream from PostgreSQL<br/>Configurable batches]
    
    H --> J[Convert to MatchTransfer]
    I --> J
    
    J --> K[queue.put<br/>Blocking if full]
    
    K --> L{Both Streams Done?}
    L -->|No| H
    L -->|Yes| M[Set done=true]
    
    B --> N[Create matchStreamSupplier<br/>Stream.generate]
    
    N --> O[queue.poll with timeout]
    O --> P{done && empty?}
    P -->|No| Q[Yield batch]
    P -->|Yes| R[End stream]
    
    Q --> O
    
    M --> S[exportAndSend<br/>Consume stream]
    R --> S
    
    S --> T[Write Parquet File]
    T --> U[Publish to Messaging System]
    
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
        B3[Create BlockingQueue]
    end
    
    subgraph "3. Parallel Streaming"
        C1[Potential Stream Thread]
        C2[Perfect Stream Thread]
        
        C1 --> C3[SELECT * FROM potential_matches<br/>WHERE group_id=? AND domain_id=?]
        C2 --> C4[SELECT * FROM perfect_matches<br/>WHERE group_id=? AND domain_id=?]
        
        C3 --> C5[ResultSet.next<br/>Configurable batches]
        C4 --> C6[ResultSet.next<br/>Configurable batches]
        
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
        E4[Compression]
        E5[File path based on identifiers]
    end
    
    subgraph "6. Messaging Publishing"
        F1[Build Message Payload]
        F2[Topic based on identifiers]
        F3[Key based on identifiers]
        F4[Send Message]
        F5{Send Success?}
        F5 -->|No| F6[Error Handling]
        F5 -->|Yes| F7[Completion]
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
    participant Messaging as ScheduleXProducer
    
    Sched->>Proc: processMatchTransfer(groupId, domain)
    
    Proc->>Proc: Create BlockingQueue
    Proc->>Proc: Setup atomic counters (recordCount, done)
    
    par Parallel Streaming
        Proc->>PotentialSvc: streamAllMatches(groupId, domainId, consumer)
        PotentialSvc->>DB: SELECT * FROM potential_matches
        DB-->>PotentialSvc: ResultSet stream
        
        loop While ResultSet.next()
            PotentialSvc->>PotentialSvc: Create PotentialMatchEntity
            PotentialSvc->>PotentialSvc: Transform to MatchTransfer
            PotentialSvc->>PotentialSvc: Accumulate to batch
            
            alt Batch ready
                PotentialSvc->>Queue: put(batch) - blocks if full
                Queue-->>PotentialSvc: Accepted
            end
        end
        
        PotentialSvc->>Queue: put(final batch)
    and
        Proc->>PerfectSvc: streamAllMatches(groupId, domainId, consumer)
        PerfectSvc->>DB: SELECT * FROM perfect_matches
        DB-->>PerfectSvc: ResultSet stream
        
        loop While ResultSet.next()
            PerfectSvc->>PerfectSvc: Create PerfectMatchEntity
            PerfectSvc->>PerfectSvc: Transform to MatchTransfer
            PerfectSvc->>PerfectSvc: Accumulate to batch
            
            alt Batch ready
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
        Export->>Queue: poll with timeout
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
    
    Proc->>Messaging: sendMessage(topic, key, payload)
    
    alt Send success
        Messaging-->>Proc: Success callback
    else Send failure
        Messaging->>Messaging: Error handling
    end
```