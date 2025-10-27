# **Scheduled Perfect Matches Creation — High-Level Design (HLD) with Diagrams**

---

## **1) Purpose and Scope**

### **Purpose Diagram**
```mermaid
graph TD
  A["Purpose"] --> B["Compute Perfect Matches"]
  A --> C["Daily Batch Execution"]
  A --> D["Consume Potential Matches"]

  B --> E["Apply Configured Algorithm"]
  B --> F["Persist to PostgreSQL"]

  C --> G["Cron-triggered (3 AM)"]
  C --> H["Avoids Real-Time Complexity"]

  D --> I["Stream from DB"]
  D --> J["Track Last Run Status"]

```

### **Scope**
- **Objective**: Daily computation and persistence of "perfect matches" for each group/domain by consuming existing potential matches.
- **Inputs**:
    - `PotentialMatchEntity` stream from PostgreSQL
    - DB-driven `MatchingConfiguration`
- **Process**:
    - Applies a selected `MatchingStrategy`
    - Tracks per-group last run status
- **Outputs**:
    - `PerfectMatchEntity` written to PostgreSQL
    - Updated `LastRunPerfectMatches` records

---

## **2) Triggers and Entry Points**

### **Trigger Flow Diagram**
```mermaid
flowchart TD
    A["Scheduler @ 3 AM"] --> B["Trigger createPerfectMatches()"]
    B --> C{"Get Tasks to Process"}
    C --> D{"Reprocess if needed?"}
    D -- Yes --> E["Execute Group Job"]
    D -- No --> F["Skip"]

```

- **Scheduler**: `PerfectMatchesCreationScheduler.createPerfectMatches`
- **Cron**: `0 0 3 * * *` (Asia/Kolkata)
- **Eligibility**: Reprocess if `processedNodes > lastRun.nodeCount` OR `lastRun.status` is `PENDING` or `FAILED`.

---

## **3) Architecture Overview**

### **High-Level Architecture Diagram**
```mermaid
graph TD
    subgraph Orchestration
        A[Scheduler] --> B[PerfectMatchCreationService]
        B --> C[JobExecutor]
    end

    subgraph MatchingPipeline
        C --> D[PerfectMatchServiceImpl]
        D --> E[PotentialMatchStreamingService]
        D --> F[MatchingStrategySelector]
        F --> G[MatchingStrategy Layer]
    end

    subgraph Persistence
        D --> H[PerfectMatchSaver]
        H --> I[PerfectMatchStorageProcessor]
        I --> J[PostgreSQL]
    end

    subgraph StateManagement
        B --> K[LastRunPerfectMatchesRepo]
        D --> L[MatchCache]
    end
```

### **Key Layers**
- **Orchestration**: Manages scheduled execution, task discovery, and concurrency control.
- **Matching Pipeline**: Streams potential matches, applies a selected strategy, and manages memory.
- **Strategy Layer**: Pluggable algorithms for perfect match computation.
- **Persistence**: Handles high-performance batch writes to PostgreSQL.

---

## **4) End-to-End Flow (Per Cycle)**

### **End-to-End Flow Diagram**
```mermaid
flowchart TD
    A[Start Scheduler Cycle] --> B[Build Tasks List]
    B --> C{For Each Task}
    C --> D[Set LastRun PENDING]
    D --> E[Clear MatchCache]
    E --> F[Acquire Semaphores]
    F --> G[Execute Group Job]
    G --> H{Success?}
    H -- Yes --> I[Set LastRun COMPLETED]
    H -- No --> J[Set LastRun FAILED]
    I --> K[Release Semaphores]
    J --> K
    K --> C
    C -- All Done --> L[Finalize Cycle]
```

### **Detailed Flow**
1. **Scheduler**: Builds a list of (domain, group) tasks.
2. **For Each Task**:
    - Updates `LastRun` status to `PENDING`.
    - Clears `MatchCache` for the group.
    - Acquires domain and group semaphores.
    - Executes the group job with retries.
3. **On Completion**:
    - Updates `LastRun` to `COMPLETED` or `FAILED`.
    - Releases semaphores.
4. **After All Tasks**: Finalizes the cycle.

---

## **5) Components and Responsibilities**

### **Component Responsibility Diagram**
```mermaid
classDiagram
    class PerfectMatchesCreationScheduler {
        +Cron trigger
        +Iterates tasks
        +@Retry/@CircuitBreaker
    }

    class PerfectMatchCreationService {
        +Discovers tasks
        +Manages concurrency
        +Persists LastRun status
    }

    class PerfectMatchCreationJobExecutor {
        +Per-group execution
        +Retry with backoff
        +Per-group semaphore
    }

    class PerfectMatchServiceImpl {
        +Streams potential matches
        +Memory-aware processing
        +Invokes MatchingStrategy
        +Persists perfect matches
    }

    class MatchingStrategySelector {
        +Resolves strategy bean
    }

    class PotentialMatchStreamingService {
        +JDBC streaming
        +Retry on SQL errors
    }

    class PerfectMatchSaver {
        +Async saving facade
        +Timeout control
    }

    class PerfectMatchStorageProcessor {
        +Batched DB writes
        +COPY + upsert
        +Transactional, @Retryable
    }
```

### **Component Responsibilities**
| **Component** | **Responsibility** |
|---------------|--------------------|
| PerfectMatchesCreationScheduler | Cron trigger, task iteration, high-level retries |
| PerfectMatchCreationService | Task discovery, concurrency management, state persistence |
| PerfectMatchCreationJobExecutor | Per-group execution with retry logic |
| PerfectMatchServiceImpl | Match computation, memory management, persistence |
| MatchingStrategySelector | Strategy resolution based on configuration |
| PotentialMatchStreamingService | High-performance data streaming from PostgreSQL |
| PerfectMatchSaver | Async saving facade with timeouts |
| PerfectMatchStorageProcessor | Batched database writes with upsert |

---

## **6) Data Flow and Persistence**

### **Data Flow Diagram**
```mermaid
graph TD
    A[PotentialMatchEntity Stream] --> B[In-Memory Transformation]
    B --> C[MatchingStrategy]
    C --> D[MatchResult]
    D --> E[PerfectMatchEntity]
    E --> F[PostgreSQL]
```

- **Inputs**:
    - `PotentialMatchEntity` stream
    - `MatchingConfiguration`
- **Transformations**:
    - Build per-node top-K adjacency
    - Apply matching strategy
- **Outputs**:
    - `PerfectMatchEntity`
    - `LastRunPerfectMatches`

### **Persistence Details**
- **Mechanism**: `COPY` to a temporary table, then `UPSERT` into the final table.
- **Batching**: Partitions input by `import.batch-size` (default 1000).
- **Resilience**: Timeout and retry mechanisms for robustness.

---

## **7) Concurrency, Backpressure, and Memory**

### **Concurrency Model Diagram**
```mermaid
graph TD
    subgraph Semaphores
        S1[domainSemaphore]
        S2[groupSemaphore]
        S3[jobExecutorSemaphore]
        S4[cpuTaskSemaphore]
    end

    subgraph Backpressure
        B1[Streaming Fetch Size]
        B2[Dynamic Sub-Batching]
        B3[Max Nodes Per Batch]
        B4[Max Matches Per Node]
    end

    subgraph Memory Guards
        M1[Memory Threshold Exceeded?]
        M2[Cancel Futures]
        M3[Shutdown CPU Executor]
    end

    S1 --> Orchestration
    S2 --> Orchestration
    S3 --> Orchestration
    S4 --> MatchingPipeline
    B1 --> MatchingPipeline
    B2 --> MatchingPipeline
    B3 --> MatchingPipeline
    B4 --> MatchingPipeline
    M1 --> MatchingPipeline
```

- **Semaphores**: Control concurrency at domain, group, job, and CPU task levels.
- **Backpressure**:
    - Streaming fetch size
    - Dynamic sub-batching based on memory
    - Limits on nodes per batch and matches per node
- **Memory Guards**:
    - Halts processing if memory threshold is exceeded
    - Cancels pending futures to protect the system

---

## **8) Matching Strategies (Pluggable)**

### **Strategy Layer Diagram**
```mermaid
classDiagram
    class MatchingStrategy {
        <<interface>>
        +match(List<PotentialMatch>)
        +supports(String)
    }

    class AuctionApproximateMatchingStrategy {
        +match()
    }

    class HopcroftKarpMatchingStrategy {
        +match()
    }

    class HungarianMatchingStrategy {
        +match()
    }

    class TopKWeightedGreedyMatchingStrategy {
        +match()
    }

    MatchingStrategy <|-- AuctionApproximateMatchingStrategy
    MatchingStrategy <|-- HopcroftKarpMatchingStrategy
    MatchingStrategy <|-- HungarianMatchingStrategy
    MatchingStrategy <|-- TopKWeightedGreedyMatchingStrategy
```

- **AuctionApproximateMatchingStrategy**: Near-optimal one-to-one assignments.
- **HopcroftKarpMatchingStrategy**: Maximum bipartite matching.
- **HungarianMatchingStrategy**: Optimal assignment.
- **TopKWeightedGreedyMatchingStrategy**: Top-K per node (memory-aware).

---

## **9) Resilience, Timeouts, Retries**

### **Resilience Mechanisms**
```mermaid
graph TD
    A[Resilience] --> B[Retries & Backoff]
    A --> C[Timeouts]
    A --> D[Circuit Breakers]
    A --> E[State Tracking]

    B --> F[Scheduler Level]
    B --> G[JobExecutor Level]
    B --> H[Streaming Level]
    B --> I[Storage Level]

    C --> J[Page Processing]
    C --> K[Save Matches]

    D --> L[Scheduler Level]

    E --> M[LastRunPerfectMatches]
```

- **Retries**: Applied at scheduler, job executor, streaming, and storage levels.
- **Timeouts**: Page processing, save matches, and top-level operations.
- **Circuit Breakers**: Wraps per-group execution at the scheduler.
- **State Tracking**: `LastRunPerfectMatches` ensures idempotent re-runs.

---

## **10) Observability**

### **Observability Dashboard**
```mermaid
graph LR
    subgraph Timers
        T1[perfect_matches_creation]
        T2[matching_duration]
        T3[perfect_match_storage_duration]
    end

    subgraph Counters
        C1[perfect_matches_creation_errors_total]
        C2[retry_attempts_total]
        C3[matching_errors_total]
        C4[perfect_matches_saved_total]
    end

    subgraph Gauges
        G1[adjacency_map_current_size]
        G2[system_cpu_usage]
    end
```

- **Metrics**: Timers, counters, and gauges for all critical operations.
- **Logging**: Consistent tagging (`groupId`, `domainId`, `cycleId`) for traceability.

---

## **11) Configuration (Selected)**

| **Category** | **Key** | **Default** |
|--------------|---------|-------------|
| Scheduling | `cron` | `0 0 3 * * *` |
| Concurrency | `match.max-concurrent-domains` | 2 |
| Retries | `match.max-retries` | 3 |
| Timeouts | `PAGE_PROCESSING_TIMEOUT_SECONDS` | 300 |
| Memory | `matching.max.memory.mb` | 1024 |
| Streaming | `BATCH_SIZE_FROM_CURSOR` | 5000 |

---

## **12) Key Sequences**

### **High-Level Sequence Diagram**
```mermaid
sequenceDiagram
    participant Scheduler
    participant Service
    participant JobExecutor
    participant ServiceImpl
    participant DB

    Scheduler->>Service: createPerfectMatches()
    Service->>JobExecutor: processGroup()
    JobExecutor->>ServiceImpl: processAndSaveMatches()
    ServiceImpl->>ServiceImpl: streamAllMatches()
    ServiceImpl->>ServiceImpl: applyStrategy()
    ServiceImpl->>DB: saveMatchesAsync()
    Service->>DB: updateLastRun()
```

---

## **13) Data and Entities**

| **Entity** | **Purpose** |
|--------------------|-------------|
| `PotentialMatchEntity` | Input data for potential matches |
| `PerfectMatchEntity` | Output data for perfect matches |
| `LastRunPerfectMatches` | State tracking for idempotency and observability |
| `MatchingConfiguration` | Drives strategy selection and behavior |

---

## **14) Risks and Considerations**

| **Risk** | **Mitigation** |
|--------------|----------------|
| Memory Safety | Graceful shutdown on memory threshold breach |
| Concurrency Layering | Verify combined semaphore limits |
| Unused Semaphore | Remove or enforce for backpressure |
| Bean Uniqueness | Ensure unique bean names for strategies |

---

## **15) Extensibility**

### **Extensibility Points**
```mermaid
graph TD
    A[Extensibility] --> B[Add New Matching Strategies]
    A --> C[Tune via Configuration]
    A --> D[Swap Data Sources]

    B --> E[Implement MatchingStrategy]
    B --> F[Register in Spring]

    C --> G[Batch sizes]
    C --> H[Timeouts]
    C --> I[Memory limits]

    D --> J[Streaming source]
    D --> K[Persistence schema]
```

- **New Strategies**: Implement `MatchingStrategy` and register in Spring.
- **Configuration**: Tune batch sizes, timeouts, and memory limits via configuration files.
- **Data Sources**: Swap streaming source or persistence schema with limited changes.

---