# Perfect Match Creation System - Low-Level Design Document

## Table of Contents
1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [Component Design](#3-component-design)
4. [Data Flow](#4-data-flow)
5. [Concurrency Model](#5-concurrency-model)
6. [Error Handling & Resilience](#6-error-handling--resilience)
7. [Performance Optimization](#7-performance-optimization)
8. [Database Design](#8-database-design)

---

## 1. System Overview

### 1.1 Purpose
The Perfect Match Creation System is a high-throughput, distributed graph processing engine designed to compute optimal matches between nodes based on compatibility scores. It processes millions of edges from LMDB storage, applies matching algorithms, and persists results to PostgreSQL.

### 1.2 Key Features
- **Scheduled Batch Processing**: Cron-based execution at configurable intervals
- **Incremental Processing**: Tracks processed nodes to avoid redundant computation
- **Concurrent Execution**: Multi-level semaphore-based concurrency control
- **Resilient Design**: Circuit breakers, retries, and graceful degradation
- **Streaming Architecture**: Memory-efficient LMDB edge streaming
- **ACID Guarantees**: PostgreSQL COPY protocol with advisory locks

---

## 2. Architecture

### 2.1 High-Level Architecture

```mermaid
graph TB
    subgraph "Scheduler Layer"
        SCH[PerfectMatchesCreationScheduler<br/>@Scheduled Cron]
    end
    
    subgraph "Orchestration Layer"
        SVC[PerfectMatchCreationService<br/>Semaphore Control]
        EXE[PerfectMatchCreationJobExecutor<br/>Retry Logic]
    end
    
    subgraph "Processing Layer"
        IMPL[PerfectMatchServiceImpl<br/>Edge Streaming & Batching]
        STRAT[MatchingStrategySelector<br/>Algorithm Selection]
    end
    
    subgraph "Storage Layer"
        EDGE[EdgePersistence<br/>LMDB Reader]
        SAVER[PerfectMatchSaver<br/>Save Queue Manager]
        STORE[PerfectMatchStorageProcessor<br/>PostgreSQL Writer]
    end
    
    subgraph "Data Sources"
        LMDB[(LMDB<br/>Edge Store)]
        PG[(PostgreSQL<br/>Match Results)]
        META[(PostgreSQL<br/>Metadata)]
    end
    
    SCH -->|Process Groups| SVC
    SVC -->|Acquire Semaphores| EXE
    EXE -->|Execute with Retry| IMPL
    IMPL -->|Select Strategy| STRAT
    IMPL -->|Stream Edges| EDGE
    IMPL -->|Save Matches| SAVER
    SAVER -->|Async Write| STORE
    
    EDGE -->|Read| LMDB
    STORE -->|COPY Protocol| PG
    SVC -->|Track Progress| META
    
    style SCH fill:#e1f5ff
    style SVC fill:#fff4e1
    style IMPL fill:#e8f5e9
    style STORE fill:#fce4ec
```

### 2.2 Component Layer Diagram

```mermaid
graph LR
    subgraph "Presentation Layer"
        A[Scheduled Trigger]
    end
    
    subgraph "Business Logic Layer"
        B[Orchestration Services]
        C[Processing Services]
        D[Algorithm Strategies]
    end
    
    subgraph "Data Access Layer"
        E[Repository Layer]
        F[Persistence Facades]
    end
    
    subgraph "Infrastructure Layer"
        G[LMDB Environment]
        H[PostgreSQL Connection Pool]
        I[Thread Pool Executors]
    end
    
    A --> B
    B --> C
    C --> D
    C --> F
    D --> E
    F --> G
    E --> H
    C --> I
    
    style A fill:#4CAF50
    style B fill:#2196F3
    style C fill:#FF9800
    style D fill:#9C27B0
    style E fill:#F44336
    style F fill:#00BCD4
    style G fill:#795548
    style H fill:#795548
    style I fill:#607D8B
```

---

## 3. Component Design

### 3.1 Scheduler Component

```mermaid
classDiagram
    class PerfectMatchesCreationScheduler {
        -MeterRegistry metrics
        -PerfectMatchCreationService service
        -AtomicBoolean running
        +createPerfectMatches() void
        +processGroupWithResilience(UUID, UUID) void
        -processGroupFallback(UUID, UUID, Throwable) void
        -isSafeToRunPerfectMatches() boolean
    }
    
    class CircuitBreaker {
        <<annotation>>
        +name: String
        +fallbackMethod: String
    }
    
    class Scheduled {
        <<annotation>>
        +cron: String
        +zone: String
    }
    
    PerfectMatchesCreationScheduler ..> CircuitBreaker
    PerfectMatchesCreationScheduler ..> Scheduled
```

**Responsibilities:**
- Trigger scheduled execution at configured cron time
- Prevent concurrent runs using `AtomicBoolean`
- Apply circuit breaker pattern for resilience
- Collect metrics for monitoring

**Key Design Decisions:**
- **Single-run guarantee**: `compareAndSet` ensures only one execution at a time
- **Async processing**: Uses `CompletableFuture.allOf` to process groups concurrently
- **Self-injection**: Uses `@Lazy` self-reference for AOP proxy (circuit breaker)

### 3.2 Service Orchestration Component

```mermaid
classDiagram
    class PerfectMatchCreationService {
        -Semaphore domainSemaphore
        -Semaphore groupSemaphore
        -ExecutorService batchExecutor
        -int maxConcurrentDomains
        -int maxConcurrentGroups
        +getTasksToProcess() List~Entry~
        +processGroup(UUID, UUID) CompletableFuture
        -processGroupTask(UUID, UUID) CompletableFuture
        +getLastRun(UUID, UUID) LastRunPerfectMatches
        +saveLastRun(LastRunPerfectMatches) void
    }
    
    class Semaphore {
        <<Java Concurrency>>
        +tryAcquire(long, TimeUnit) boolean
        +release() void
        +availablePermits() int
    }
    
    class LastRunPerfectMatches {
        -UUID id
        -UUID groupId
        -UUID domainId
        -Long nodeCount
        -String status
        -LocalDateTime runDate
    }
    
    PerfectMatchCreationService --> "2" Semaphore
    PerfectMatchCreationService ..> LastRunPerfectMatches
```

**Concurrency Control:**
```java
// Two-level semaphore hierarchy
domainSemaphore (permits=2)  // Max 2 domains concurrently
    ├─> groupSemaphore (permits=1)  // Max 1 group per domain
```

**Task Selection Logic:**
```mermaid
flowchart TD
    A[Start] --> B[Get Active Domains]
    B --> C[For Each Domain]
    C --> D[Get Group IDs]
    D --> E[For Each Group]
    E --> F{Get Last Run}
    F --> G[Get Total Node Count]
    G --> H{Is Fully Processed?}
    H -->|Yes| I{Last Status<br/>FAILED/PENDING?}
    H -->|No| J[Add to Task List]
    I -->|Yes| J
    I -->|No| K[Skip]
    J --> L{More Groups?}
    K --> L
    L -->|Yes| E
    L -->|No| M{More Domains?}
    M -->|Yes| C
    M -->|No| N[Return Tasks]
```

### 3.3 Job Executor Component

```mermaid
sequenceDiagram
    participant Executor as PerfectMatchCreationJobExecutor
    participant Service as PerfectMatchService
    participant Retry as Retry Logic
    
    Executor->>Executor: processGroup(groupId, domainId)
    Executor->>Retry: processGroupWithRetriesAsync()
    
    loop Attempt 1 to maxRetries (3)
        Retry->>Retry: Build MatchingRequest with cycleId
        Retry->>Service: processAndSaveMatches(request)
        
        alt Success
            Service-->>Retry: CompletableFuture<Void> success
            Retry-->>Executor: Complete successfully
        else Failure
            Service-->>Retry: Exception
            Retry->>Retry: Sleep with exponential backoff
            Note over Retry: delay = 1000ms * 2^(attempt-1)
            Retry->>Retry: Increment attempt counter
        end
    end
    
    alt Max Retries Exceeded
        Retry-->>Executor: CompleteExceptionally
    end
```

**Retry Strategy:**
| Attempt | Delay     | Formula                    |
|---------|-----------|----------------------------|
| 1       | 0ms       | Initial attempt            |
| 2       | 1000ms    | 1000 * 2^0                |
| 3       | 2000ms    | 1000 * 2^1                |
| Failure | -         | CompleteExceptionally      |

### 3.4 Processing Engine Component

```mermaid
classDiagram
    class PerfectMatchServiceImpl {
        -EdgePersistence edgePersistence
        -MatchingStrategySelector strategySelector
        -NodeRepository nodeRepo
        -ExecutorService cpuExecutor
        -int topK
        +processAndSaveMatches(request) CompletableFuture
        -buildMatchingContext(groupId, domainId) MatchingContext
        -runPerfectMatchFromLmdb(...) CompletableFuture
        -processEdgeBatch(...) void
    }
    
    class MatchingStrategySelector {
        -Map~String, MatchingStrategy~ strategyMap
        +select(context, groupId) MatchingStrategy
    }
    
    class MatchingStrategy {
        <<interface>>
        +match(potentialMatches, groupId, domainId) Map
    }
    
    class EdgePersistence {
        <<interface>>
        +streamEdges(domainId, groupId) AutoCloseableStream
    }
    
    PerfectMatchServiceImpl --> MatchingStrategySelector
    PerfectMatchServiceImpl --> EdgePersistence
    MatchingStrategySelector --> MatchingStrategy
```

**Processing Pipeline:**

```mermaid
flowchart LR
    A[Build Context] --> B{Node Count Check}
    B -->|Count = 0| C[Skip & Return]
    B -->|Count <= Last| C
    B -->|Count > Last| D[Stream Edges from LMDB]
    D --> E[Buffer Edges<br/>BATCH_SIZE=25000]
    E --> F[Create Edge Batch]
    F --> G[Submit to CPU Executor]
    G --> H[Build Adjacency Map]
    H --> I[Trim to Top-K]
    I --> J[Apply Matching Strategy]
    J --> K[Convert to Entities]
    K --> L[Async Save to PostgreSQL]
    L --> M{More Edges?}
    M -->|Yes| E
    M -->|No| N[Wait All Batches]
    N --> O[Complete]
    
    style B fill:#FFF59D
    style D fill:#B2DFDB
    style J fill:#F8BBD0
    style L fill:#BBDEFB
```

---

## 4. Data Flow

### 4.1 End-to-End Data Flow

```mermaid
graph TB
    subgraph "1. Initialization"
        A1[Cron Trigger] -->|01:28 IST| A2[Get Tasks to Process]
        A2 --> A3[Query LastRunPerfectMatches]
        A2 --> A4[Count Total Nodes]
    end
    
    subgraph "2. Semaphore Acquisition"
        B1[Domain Semaphore<br/>tryAcquire 15min]
        B2[Group Semaphore<br/>tryAcquire 240min]
        B1 --> B2
    end
    
    subgraph "3. Context Building"
        C1[Get MatchingConfiguration]
        C2[Determine MatchType<br/>SYMMETRIC/ASYMMETRIC]
        C3[Build MatchingContext<br/>with processed node count]
        C1 --> C2 --> C3
    end
    
    subgraph "4. Edge Streaming"
        D1[Open LMDB Txn<br/>Read-only]
        D2[Create Cursor<br/>KeyRange.atLeast prefix]
        D3[Stream EdgeDTO Objects]
        D4[Buffer 25k Edges]
        D1 --> D2 --> D3 --> D4
    end
    
    subgraph "5. Batch Processing"
        E1[Build Adjacency Map]
        E2[Trim to Top-K=100]
        E3[Execute Strategy.match]
        E4[Generate MatchResults]
        E1 --> E2 --> E3 --> E4
    end
    
    subgraph "6. Persistence"
        F1[Convert to PerfectMatchEntity]
        F2[Acquire Save Semaphore<br/>permits=2]
        F3[Acquire Advisory Lock<br/>on groupId]
        F4[PostgreSQL COPY Protocol]
        F5[Upsert from Temp Table]
        F1 --> F2 --> F3 --> F4 --> F5
    end
    
    subgraph "7. Completion"
        G1[Update LastRunPerfectMatches<br/>status=COMPLETED]
        G2[Release Semaphores]
        G3[Record Metrics]
    end
    
    A3 --> B1
    A4 --> B1
    B2 --> C1
    C3 --> D1
    D4 --> E1
    E4 --> F1
    F5 --> G1
    G1 --> G2
    G2 --> G3
    
    style A2 fill:#E8F5E9
    style B1 fill:#FFF3E0
    style C3 fill:#E1F5FE
    style D3 fill:#F3E5F5
    style E3 fill:#FCE4EC
    style F4 fill:#FFEBEE
    style G1 fill:#E0F2F1
```

### 4.2 Edge Streaming Detail

```mermaid
sequenceDiagram
    participant Client as PerfectMatchServiceImpl
    participant Facade as EdgePersistenceFacade
    participant Reader as LmdbEdgeReader
    participant LMDB as LMDB Database
    
    Client->>Facade: streamEdges(domainId, groupId)
    Facade->>Facade: buildGroupPrefix(groupId)
    Note over Facade: Prefix = groupId (16 bytes)
    
    Facade->>Reader: streamEdges(domainId, prefix, matcher)
    Reader->>LMDB: env.txnRead()
    LMDB-->>Reader: Txn<ByteBuffer>
    
    Reader->>LMDB: dbi.iterate(txn, KeyRange.atLeast(prefix))
    LMDB-->>Reader: CursorIterable
    
    loop For Each Key-Value Pair
        Reader->>Reader: Check prefix matches
        alt Prefix Matches
            Reader->>Reader: Check domainId matches (bytes 4-20)
            alt Domain Matches
                Reader->>Reader: Decode EdgeDTO
                Reader-->>Client: yield EdgeDTO
            else Domain Mismatch
                Reader->>Reader: Skip
            end
        else Prefix Mismatch
            Reader->>Reader: Stop iteration
        end
    end
    
    Client->>Client: Close AutoCloseableStream
    Client->>Reader: close()
    Reader->>LMDB: cursor.close()
    Reader->>LMDB: txn.close()
```

**LMDB Key Structure:**
```
┌─────────────────┬─────────────────┬─────────────────────┐
│   Group ID      │   Cycle Hash    │   Node Pair Hash    │
│   16 bytes      │   32 bytes      │   32 bytes          │
│   (2 longs)     │   (SHA-256)     │   (SHA-256)         │
└─────────────────┴─────────────────┴─────────────────────┘
      UUID           cycleId hash      sorted(a,b) hash
   MSB + LSB        deterministic     deterministic
```

**LMDB Value Structure:**
```
┌────────┬─────────────┬──────────┬──────────┬────────┬──────────┬────────┐
│ Score  │  Domain ID  │ FromLen  │ FromHash │ ToLen  │ ToHash   │        │
│ 4 byte │  16 bytes   │ 4 bytes  │ N bytes  │4 bytes │ M bytes  │        │
│ float  │   UUID      │  int     │  UTF-8   │ int    │  UTF-8   │        │
└────────┴─────────────┴──────────┴──────────┴────────┴──────────┴────────┘
```

### 4.3 Matching Algorithm Flow

```mermaid
flowchart TD
    A[Receive Edge Batch<br/>25,000 edges] --> B[Initialize Adjacency Map<br/>HashMap~String, PriorityQueue~]
    
    B --> C{For Each Edge}
    C --> D[Extract: from, to, score]
    D --> E[Add Forward Edge<br/>from → to]
    E --> F[Add Reverse Edge<br/>to → from]
    F --> G{More Edges?}
    G -->|Yes| C
    G -->|No| H[Trim Each Node's Queue<br/>Keep Top-K=100]
    
    H --> I[Flatten All PotentialMatches<br/>to List]
    I --> J[Call strategy.match]
    
    J --> K{Strategy Type}
    K -->|Symmetric| L[Symmetric Matching<br/>Mutual Best Match]
    K -->|Asymmetric| M[Asymmetric Matching<br/>One-way Preference]
    
    L --> N[Filter & Rank Results]
    M --> N
    
    N --> O[Return Map~String, List~MatchResult~~]
    O --> P[Convert to PerfectMatchEntity]
    P --> Q[Async Save]
    
    style A fill:#E3F2FD
    style J fill:#FFF9C4
    style L fill:#F3E5F5
    style M fill:#F3E5F5
    style Q fill:#FFEBEE
```

---

## 5. Concurrency Model

### 5.1 Thread Pool Architecture

```mermaid
graph TB
    subgraph "Scheduler Thread"
        ST[Spring @Scheduled Thread<br/>sched-1]
    end
    
    subgraph "Match Creation Executor Pool"
        MCE1[match-create-1]
        MCE2[match-create-2]
        MCE3[match-create-N]
        MCE[ThreadPoolExecutor<br/>matchCreationExecutorService]
        MCE --> MCE1
        MCE --> MCE2
        MCE --> MCE3
    end
    
    subgraph "CPU-Bound Executor Pool"
        CPU1[pool-13-thread-1]
        CPU2[pool-13-thread-2]
        CPU3[pool-13-thread-N]
        CPUE[FixedThreadPool<br/>size=max 4, availableProcessors]
        CPUE --> CPU1
        CPUE --> CPU2
        CPUE --> CPU3
    end
    
    subgraph "I/O Executor Pool"
        IO1[io-thread-1]
        IO2[io-thread-2]
        IO3[io-thread-N]
        IOE[ThreadPoolExecutor<br/>ioExecutorService]
        IOE --> IO1
        IOE --> IO2
        IOE --> IO3
    end
    
    ST -->|Submit Group Tasks| MCE
    MCE1 -->|Process Edges| CPUE
    MCE2 -->|Process Edges| CPUE
    CPU1 -->|Save Matches| IOE
    CPU2 -->|Save Matches| IOE
    
    style ST fill:#4CAF50
    style MCE fill:#2196F3
    style CPUE fill:#FF9800
    style IOE fill:#9C27B0
```

### 5.2 Semaphore Control Flow

```mermaid
stateDiagram-v2
    [*] --> Queued: Task Submitted
    
    Queued --> WaitDomainSem: Start Execution
    WaitDomainSem --> DomainAcquired: tryAcquire(15min)
    WaitDomainSem --> Timeout1: Timeout
    Timeout1 --> [*]: Throw TimeoutException
    
    DomainAcquired --> WaitGroupSem: Continue
    WaitGroupSem --> GroupAcquired: tryAcquire(240min)
    WaitGroupSem --> Timeout2: Timeout
    Timeout2 --> ReleaseDomain: Release Domain Semaphore
    ReleaseDomain --> [*]: Throw TimeoutException
    
    GroupAcquired --> Processing: Execute Job
    Processing --> Success: Job Complete
    Processing --> Failure: Job Failed
    
    Success --> ReleaseGroup: Release Group Semaphore
    Failure --> ReleaseGroup
    ReleaseGroup --> ReleaseDomain2: Release Domain Semaphore
    ReleaseDomain2 --> [*]: Complete
```

**Semaphore Configuration:**

| Semaphore | Permits | Timeout | Purpose |
|-----------|---------|---------|---------|
| `domainSemaphore` | 2 | 15 min | Limit concurrent domains |
| `groupSemaphore` | 1 | 240 min | Serialize groups per domain |
| `saveSemaphore` | 2 | N/A | Limit concurrent DB writes |
| `storageSemaphore` | 16 | N/A | Limit storage processor concurrency |

### 5.3 Concurrency Patterns

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant T1 as Task Group1
    participant T2 as Task Group2
    participant DS as Domain Semaphore<br/>(permits=2)
    participant GS as Group Semaphore<br/>(permits=1)
    participant CPU as CPU Executor
    
    S->>T1: Process Group1
    S->>T2: Process Group2
    
    par Concurrent Domain Processing
        T1->>DS: tryAcquire()
        DS-->>T1: Acquired (1/2)
        T1->>GS: tryAcquire()
        GS-->>T1: Acquired (0/1)
        
        T1->>CPU: Submit 24 edge batches
        
        loop Process Batches
            CPU->>CPU: Build adjacency
            CPU->>CPU: Match algorithm
            CPU->>CPU: Save async
        end
        
        CPU-->>T1: All batches done
        T1->>GS: release()
        T1->>DS: release()
    and
        T2->>DS: tryAcquire()
        DS-->>T2: Acquired (0/2)
        T2->>GS: tryAcquire()
        Note over T2,GS: Blocks until T1 releases
        GS-->>T2: Acquired (0/1)
        
        T2->>CPU: Submit batches
        CPU-->>T2: Complete
        T2->>GS: release()
        T2->>DS: release()
    end
```

---

## 6. Error Handling & Resilience

### 6.1 Resilience Architecture

```mermaid
graph TB
    subgraph "Layer 1: Circuit Breaker"
        CB[Resilience4j CircuitBreaker<br/>perfectMatchesGroup]
        CB -->|Open| FB1[Fallback: Update Status FAILED]
        CB -->|Half-Open| RETRY1[Allow Limited Requests]
        CB -->|Closed| PROCEED1[Normal Processing]
    end
    
    subgraph "Layer 2: Retry Mechanism"
        RT[Exponential Backoff Retry<br/>maxAttempts=3]
        RT -->|Attempt 1| EXEC1[Execute]
        RT -->|Attempt 2<br/>delay=1s| EXEC2[Execute]
        RT -->|Attempt 3<br/>delay=2s| EXEC3[Execute]
        EXEC3 -->|Still Fails| FB2[CompleteExceptionally]
    end
    
    subgraph "Layer 3: Timeout Protection"
        TO[CompletableFuture Timeout]
        TO -->|Semaphore: 15min| TO1[Domain Lock Timeout]
        TO -->|Semaphore: 240min| TO2[Group Lock Timeout]
        TO -->|Save: 30min| TO3[Storage Timeout]
    end
    
    subgraph "Layer 4: Database Resilience"
        DB[Database Layer]
        DB -->|Deadlock| DBR[@Retryable with backoff]
        DB -->|Advisory Lock| AL[PostgreSQL pg_try_advisory_lock]
        DB -->|COPY Failure| CANCEL[CopyIn.cancelCopy]
    end
    
    PROCEED1 --> RT
    RT --> TO
    TO --> DB
    
    style CB fill:#FFCDD2
    style RT fill:#F8BBD0
    style TO fill:#E1BEE7
    style DB fill:#C5CAE9
```

### 6.2 Error Handling Flow

```mermaid
flowchart TD
    A[Start Processing] --> B{Circuit Breaker State}
    B -->|OPEN| C[Immediate Fallback<br/>Update Status FAILED]
    B -->|CLOSED/HALF_OPEN| D[Attempt Execution]
    
    D --> E{Try Acquire<br/>Domain Semaphore}
    E -->|Timeout 15min| F[TimeoutException]
    E -->|Success| G{Try Acquire<br/>Group Semaphore}
    
    G -->|Timeout 240min| H[Release Domain<br/>TimeoutException]
    G -->|Success| I[Execute Job]
    
    I --> J{Exception?}
    J -->|Yes| K{Retry Count < 3?}
    K -->|Yes| L[Exponential Backoff<br/>Sleep 2^n seconds]
    L --> I
    K -->|No| M[Max Retries Exceeded]
    
    J -->|No| N[Success Path]
    
    M --> O[Update Status FAILED]
    N --> P[Update Status COMPLETED]
    
    O --> Q[Release Semaphores]
    P --> Q
    H --> Q
    F --> Q
    C --> R[End]
    Q --> R
    
    style F fill:#FFCDD2
    style H fill:#FFCDD2
    style M fill:#FFCDD2
    style O fill:#FFCDD2
    style P fill:#C8E6C9
```

### 6.3 Exception Hierarchy

```mermaid
classDiagram
    class Throwable {
        <<Java Core>>
    }
    
    class Exception {
        <<Java Core>>
    }
    
    class RuntimeException {
        <<Java Core>>
    }
    
    class SQLException {
        <<Java Core>>
    }
    
    class CompletionException {
        <<Java Core>>
        +getCause() Throwable
    }
    
    class TimeoutException {
        <<Java Core>>
    }
    
    class IllegalStateException {
        <<Java Core>>
    }
    
    class BadRequestException {
        <<Custom>>
        -String message
    }
    
    Throwable <|-- Exception
    Exception <|-- RuntimeException
    Exception <|-- SQLException
    RuntimeException <|-- CompletionException
    RuntimeException <|-- IllegalStateException
    Exception <|-- TimeoutException
    RuntimeException <|-- BadRequestException
```

**Exception Handling Strategy:**

| Exception Type | Handling Strategy | Retry | Fallback |
|----------------|-------------------|-------|----------|
| `TimeoutException` | Release semaphores, log error | No | Set status FAILED |
| `SQLException` (Deadlock) | `@Retryable` with backoff | Yes (3x) | Rollback transaction |
| `CompletionException` | Unwrap cause, log, propagate | Depends on cause | Circuit breaker fallback |
| `InterruptedException` | Restore interrupt flag, fail fast | No | Set status FAILED |
| `IllegalStateException` | Log error, complete exceptionally | No | Return empty result |

---

## 7. Performance Optimization

### 7.1 Batch Processing Strategy

```mermaid
graph LR
    subgraph "Edge Streaming"
        A[LMDB Cursor<br/>No memory limit] -->|Stream| B[Buffer<br/>25,000 edges]
    end
    
    subgraph "CPU-Bound Processing"
        B --> C[Batch 1<br/>Build Adjacency]
        B --> D[Batch 2<br/>Build Adjacency]
        B --> E[Batch N<br/>Build Adjacency]
        
        C --> F[Trim Top-100]
        D --> G[Trim Top-100]
        E --> H[Trim Top-100]
        
        F --> I[Match Algorithm]
        G --> J[Match Algorithm]
        H --> K[Match Algorithm]
    end
    
    subgraph "I/O-Bound Persistence"
        I --> L[Async Save 1]
        J --> M[Async Save 2]
        K --> N[Async Save N]
        
        L --> O[PostgreSQL COPY]
        M --> O
        N --> O
    end
    
    style B fill:#FFF59D
    style C fill:#B2DFDB
    style D fill:#B2DFDB
    style E fill:#B2DFDB
    style O fill:#FFCCBC
```

### 7.2 Memory Management

**Heap Memory Profile:**

```mermaid
pie title Memory Allocation by Component
    "Edge Buffers (ArrayList 25k)" : 35
    "Adjacency Maps (HashMap)" : 25
    "Priority Queues (Top-K)" : 15
    "Entity Collections" : 15
    "LMDB ByteBuffers (Direct)" : 5
    "Connection Pool" : 3
    "Misc Overhead" : 2
```

**Memory Optimization Techniques:**

| Technique | Implementation | Benefit |
|-----------|----------------|---------|
| **Direct ByteBuffers** | LMDB `ThreadLocal<ByteBuffer>` | Off-heap, no GC pressure |
| **Batch Clearing** | `buffer.clear()` after copy | Reuse ArrayList capacity |
| **Priority Queue Trimming** | `while (pq.size() > topK) pq.poll()` | Cap memory per node |
| **Stream Processing** | `AutoCloseableStream` | No full graph in memory |
| **Copy-on-Submit** | `List.copyOf(buffer)` | Immutable batches |

### 7.3 Database Optimization

```mermaid
sequenceDiagram
    participant App as Application
    participant Pool as HikariCP Connection Pool
    participant PG as PostgreSQL
    
    App->>Pool: getConnection()
    Pool-->>App: Connection (from pool)
    
    App->>PG: BEGIN
    App->>PG: SET LOCAL statement_timeout = 1500s
    App->>PG: SET LOCAL synchronous_commit = off
    App->>PG: CREATE TEMP TABLE temp_perfect_matches
    
    loop For Each Batch (1000 records)
        App->>PG: COPY temp_perfect_matches FROM STDIN (BINARY)
        Note over App,PG: Binary protocol - 10x faster than INSERT
    end
    
    App->>PG: INSERT INTO perfect_matches<br/>ON CONFLICT DO UPDATE<br/>FROM temp_perfect_matches
    App->>PG: COMMIT
    
    App->>Pool: close() - Return to pool
```

**PostgreSQL Optimizations:**

| Setting | Value | Impact |
|---------|-------|--------|
| `synchronous_commit` | `off` | 2-3x write throughput |
| `statement_timeout` | `1500s` | Prevent indefinite locks |
| `lock_timeout` | `10s` | Fast-fail on contention |
| Binary COPY | Enabled | 10x faster than text INSERT |
| Advisory Locks | Group-level | Prevent concurrent writes |
| Temp Tables | Session-scoped | No WAL overhead |

### 7.4 Throughput Metrics

**Target Performance:**

```
┌─────────────────────────────────────────────────────────┐
│ Metric                    │ Value                       │
├───────────────────────────┼─────────────────────────────┤
│ Edge Stream Rate          │ ~70,000 edges/sec          │
│ Batch Processing          │ 25,000 edges/batch         │
│ Concurrent Batches        │ 4-16 (CPU-bound)           │
│ Match Generation Rate     │ ~50,000 matches/sec        │
│ DB Write Rate             │ ~30,000 inserts/sec (COPY) │
│ End-to-End Latency        │ ~10 sec for 577k edges     │
│ Memory Footprint          │ <2GB heap per group        │
└─────────────────────────────────────────────────────────┘
```

---

## 8. Database Design

### 8.1 Schema Design

```mermaid
erDiagram
    PERFECT_MATCHES ||--o{ LAST_RUN_PERFECT_MATCHES : tracks
    PERFECT_MATCHES ||--|| MATCHING_GROUPS : belongs_to
    MATCHING_GROUPS ||--|| MATCHING_CONFIGURATIONS : has
    MATCHING_CONFIGURATIONS ||--|| ALGORITHMS : uses
    
    PERFECT_MATCHES {
        uuid id PK
        uuid group_id FK
        uuid domain_id
        uuid processing_cycle_id
        string reference_id
        string matched_reference_id
        float compatibility_score
        timestamp matched_at
        index idx_group_domain
        index idx_reference
        unique uq_group_ref_matched
    }
    
    LAST_RUN_PERFECT_MATCHES {
        uuid id PK
        uuid group_id FK
        uuid domain_id
        bigint node_count
        string status
        timestamp run_date
        unique uq_domain_group
    }
    
    MATCHING_GROUPS {
        uuid id PK
        uuid domain_id
        string name
        boolean cost_based
        string industry
        boolean active
    }
    
    MATCHING_CONFIGURATIONS {
        uuid id PK
        uuid group_id FK
        uuid algorithm_id FK
        int priority
        jsonb config_params
    }
    
    ALGORITHMS {
        string id PK
        string name
        string description
        string strategy_class
    }
```

### 8.2 Index Strategy

```sql
-- Primary lookup patterns
CREATE INDEX CONCURRENTLY idx_pm_group_domain 
    ON perfect_matches(group_id, domain_id);

CREATE INDEX CONCURRENTLY idx_pm_reference 
    ON perfect_matches(reference_id) 
    WHERE matched_at > NOW() - INTERVAL '30 days';

CREATE INDEX CONCURRENTLY idx_pm_cycle 
    ON perfect_matches(processing_cycle_id);

-- Prevent duplicates
CREATE UNIQUE INDEX uq_pm_group_ref_matched 
    ON perfect_matches(group_id, reference_id, matched_reference_id);

-- Last run tracking
CREATE UNIQUE INDEX uq_last_run_domain_group 
    ON last_run_perfect_matches(domain_id, group_id);

-- Partial index for active groups
CREATE INDEX idx_groups_active 
    ON matching_groups(domain_id, id) 
    WHERE active = true;
```

### 8.3 Transaction Isolation

```mermaid
sequenceDiagram
    participant T1 as Transaction 1<br/>(Group A)
    participant T2 as Transaction 2<br/>(Group A)
    participant PG as PostgreSQL
    
    T1->>PG: SELECT pg_try_advisory_lock('group-a-hash')
    PG-->>T1: true (lock acquired)
    
    T2->>PG: SELECT pg_try_advisory_lock('group-a-hash')
    PG-->>T2: false (lock held by T1)
    T2->>T2: Wait or fail
    
    T1->>PG: BEGIN
    T1->>PG: COPY temp_perfect_matches
    T1->>PG: INSERT ... ON CONFLICT DO UPDATE
    T1->>PG: COMMIT
    T1->>PG: RESET ALL (releases advisory lock)
    
    Note over T2: T1 committed, lock released
    T2->>PG: SELECT pg_try_advisory_lock('group-a-hash')
    PG-->>T2: true (lock acquired)
```

**Lock Hierarchy:**

```
Application Semaphores (JVM-level)
    ↓
PostgreSQL Advisory Locks (DB-level)
    ↓
Row-level Locks (Automatic)
```

---

## 9. Monitoring & Observability

### 9.1 Metrics Collection

```mermaid
graph TB
    subgraph "Application Metrics"
        M1[perfect_match_duration_seconds<br/>Timer]
        M2[perfect_matches_saved_total<br/>Counter]
        M3[perfect_match_batch_errors<br/>Counter]
        M4[retry_attempts_total<br/>Counter]
        M5[max_retries_exceeded<br/>Counter]
    end
    
    subgraph "JVM Metrics"
        J1[jvm_memory_used_bytes<br/>Gauge]
        J2[jvm_gc_pause_seconds<br/>Timer]
        J3[jvm_threads_states<br/>Gauge]
    end
    
    subgraph "System Metrics"
        S1[system_cpu_usage<br/>Gauge]
        S2[process_cpu_usage<br/>Gauge]
        S3[hikaricp_connections_active<br/>Gauge]
    end
    
    M1 --> REG[MeterRegistry]
    M2 --> REG
    M3 --> REG
    M4 --> REG
    M5 --> REG
    J1 --> REG
    J2 --> REG
    J3 --> REG
    S1 --> REG
    S2 --> REG
    S3 --> REG
    
    REG --> PROM[Prometheus Exporter]
    PROM --> GRAF[Grafana Dashboards]
    
    style REG fill:#4CAF50
    style PROM fill:#FF5722
    style GRAF fill:#2196F3
```

### 9.2 Key Metrics

| Metric Name | Type | Labels | Purpose |
|-------------|------|--------|---------|
| `perfect_match_duration_seconds` | Timer | groupId, domainId, cycleId, status | End-to-end latency |
| `perfect_matches_saved_total` | Counter | groupId, domainId, cycleId | Throughput tracking |
| `perfect_match_batch_errors` | Counter | groupId, domainId, cycleId | Error rate |
| `retry_attempts_total` | Counter | groupId, domainId, attempt, errorType | Retry frequency |
| `batch_perfect_matches_duration` | Timer | groupId, domainId | Per-batch timing |
| `hikaricp_connections_active` | Gauge | pool | DB connection usage |

### 9.3 Logging Strategy

```
┌─────────────────────────────────────────────────────────────┐
│ Log Level │ Event                                           │
├───────────┼─────────────────────────────────────────────────┤
│ INFO      │ - Job start/completion                          │
│           │ - Semaphore acquisition/release                 │
│           │ - Batch submission counts                       │
│           │ - Match save confirmations                      │
├───────────┼─────────────────────────────────────────────────┤
│ WARN      │ - Node count = 0                                │
│           │ - Retry attempts                                │
│           │ - Slow queries (>1s)                            │
├───────────┼─────────────────────────────────────────────────┤
│ ERROR     │ - Semaphore timeout                             │
│           │ - Max retries exceeded                          │
│           │ - Database failures                             │
│           │ - COPY protocol errors                          │
├───────────┼─────────────────────────────────────────────────┤
│ DEBUG     │ - Individual batch processing                   │
│           │ - Edge iteration details                        │
│           │ - Match result details                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 10. Deployment Considerations

### 10.1 Configuration Properties

```yaml
# Scheduling
scheduling:
  perfect-match:
    cron: "0 28 1 * * *"  # 01:28 IST daily
    zone: "Asia/Kolkata"

# Concurrency Control
match:
  max-concurrent-domains: 2
  max-concurrent-groups: 1
  max-retries: 3
  retry-delay-millis: 1000

# Processing
matching:
  topk:
    count: 100

perfectmatch:
  node-stability-minutes: 10

# Database
import:
  batch-size: 1000

spring:
  datasource:
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 30000
      idle-timeout: 600000
      max-lifetime: 1800000

# Resilience4j
resilience4j:
  circuitbreaker:
    instances:
      perfectMatchesGroup:
        failure-rate-threshold: 50
        wait-duration-in-open-state: 60s
        sliding-window-size: 10
        permitted-number-of-calls-in-half-open-state: 3
```

### 10.2 Resource Requirements

```
┌────────────────────────────────────────────────────┐
│ Resource          │ Minimum    │ Recommended      │
├───────────────────┼────────────┼──────────────────┤
│ JVM Heap          │ 4GB        │ 8GB              │
│ CPU Cores         │ 4          │ 8-16             │
│ LMDB Disk         │ 50GB SSD   │ 200GB NVMe       │
│ PostgreSQL        │ 16GB RAM   │ 32GB RAM         │
│ Network           │ 1 Gbps     │ 10 Gbps          │
│ Connection Pool   │ 10 conns   │ 20 conns         │
└────────────────────────────────────────────────────┘
```

---

## 11. Future Enhancements

1. **Distributed Processing**: Implement Apache Kafka for task distribution across multiple nodes
2. **Incremental Updates**: Delta processing instead of full recomputation
3. **Caching Layer**: Redis for frequently accessed match results
4. **ML Integration**: Use machine learning models for match scoring
5. **Real-time Streaming**: Kafka Streams for continuous match updates
6. **GraphQL API**: Query interface for match exploration
7. **A/B Testing**: Framework for algorithm comparison

---

## Appendix A: Sequence Diagram - Complete Flow

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler
    participant SVC as CreationService
    participant EXE as JobExecutor
    participant IMPL as ServiceImpl
    participant EDGE as EdgePersistence
    participant LMDB as LMDB Store
    participant STRAT as MatchingStrategy
    participant SAVER as MatchSaver
    participant STORE as StorageProcessor
    participant PG as PostgreSQL
    
    SCH->>SCH: @Scheduled Trigger (01:28 IST)
    SCH->>SVC: getTasksToProcess()
    SVC->>PG: Query LastRunPerfectMatches
    SVC->>PG: Count Nodes
    PG-->>SVC: Tasks List
    
    loop For Each Group
        SCH->>SVC: processGroup(groupId, domainId)
        SVC->>SVC: tryAcquire domainSemaphore (15min)
        SVC->>SVC: tryAcquire groupSemaphore (240min)
        
        SVC->>EXE: processGroup(groupId, domainId)
        
        loop Retry up to 3 times
            EXE->>IMPL: processAndSaveMatches(request)
            IMPL->>PG: Get LastRun & Node Count
            
            alt Node Count > Last Processed
                IMPL->>EDGE: streamEdges(domainId, groupId)
                EDGE->>LMDB: Open Txn, Create Cursor
                
                loop Stream Edges
                    LMDB-->>EDGE: EdgeDTO
                    EDGE-->>IMPL: EdgeDTO
                    IMPL->>IMPL: Buffer until 25k
                    
                    alt Buffer Full
                        IMPL->>IMPL: Submit Batch to cpuExecutor
                        
                        par Process Batch
                            IMPL->>IMPL: Build Adjacency Map
                            IMPL->>IMPL: Trim to Top-K
                            IMPL->>STRAT: match(potentialMatches)
                            STRAT-->>IMPL: Map<String, List<MatchResult>>
                            IMPL->>IMPL: Convert to Entities
                            IMPL->>SAVER: saveMatchesAsync()
                            
                            SAVER->>STORE: savePerfectMatches()
                            STORE->>PG: Acquire Advisory Lock
                            STORE->>PG: CREATE TEMP TABLE
                            STORE->>PG: COPY Binary Data
                            STORE->>PG: UPSERT from Temp
                            STORE->>PG: COMMIT
                            STORE->>PG: Release Advisory Lock
                        end
                    end
                end
                
                IMPL->>IMPL: Wait for all batches
                IMPL-->>EXE: Success
                EXE-->>SVC: Success
            else
                IMPL-->>EXE: Skip (no new nodes)
            end
        end
        
        SVC->>PG: Update LastRun (status=COMPLETED)
        SVC->>SVC: Release groupSemaphore
        SVC->>SVC: Release domainSemaphore
    end
    
    SCH->>SCH: Job Complete
```

---