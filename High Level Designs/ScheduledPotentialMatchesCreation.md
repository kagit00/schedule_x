# **Scheduled Potential Match Creation — High-Level Design (HLD) with Diagrams**

---

## **1) Purpose and Scope**

### **Purpose Diagram**
```mermaid
graph TD
    A[Purpose] --> B[Compute Potential Matches]
    A --> C[Batch-Oriented Pipeline]
    A --> D[Strong Backpressure & Resilience]

    B --> E[Between Nodes by Domain/Group]
    B --> F[Configurable Graph Strategies]

    D --> G[Semaphores for Concurrency Control]
    D --> H[Bounded Queues for Backpressure]
    D --> I[Resilience with Retries & Circuit Breakers]
```

### **Scope**
- **Inputs**: Nodes with metadata and relationships
- **Process**: Compute potential matches using configurable graph strategies
- **Outputs**:
    - Transient edges stored in MapDB for staging/streaming
    - Finalized potential matches upserted into PostgreSQL
- **Constraints**:
    - Batch-oriented processing
    - Strong backpressure and resilience mechanisms

---

## **2) Architecture Overview**

### **High-Level Architecture Diagram**
```mermaid
graph TD
    subgraph Orchestration
        A[Scheduler] --> B[JobExecutor]
    end

    subgraph MatchingPipeline
        B --> C[PotentialMatchService]
        C --> D[NodeFetchService]
        C --> E[WeightFunctionResolver]
        C --> F[GraphPreProcessor]
        F --> G[SymmetricGraphBuilder]
        F --> H[BipartiteGraphBuilder]
    end

    subgraph PersistencePipeline
        G --> I[PotentialMatchComputationProcessor]
        H --> I
        I --> J[QueueManager]
        J --> K[GraphStore (MapDB)]
        J --> L[PotentialMatchSaver]
        L --> M[PostgreSQL]
    end

    subgraph Finalization
        I --> N[MatchesCreationFinalizer]
    end
```

### **Key Components**
- **Orchestration**: Scheduler, JobExecutor
- **Matching Pipeline**: Node fetching, weight function resolution, graph preprocessing, and building
- **Persistence Pipeline**: Queue management, MapDB staging, PostgreSQL persistence
- **Finalization**: Cleanup and completion signaling

---

## **3) End-to-End Flow**

### **End-to-End Flow Diagram**
```mermaid
flowchart TD
    A[Start Scheduled Cycle] --> B[Enumerate Active Domains/Groups]
    B --> C[Acquire Semaphores]
    C --> D[Run Per-Group Job]
    D --> E[Fetch Node IDs]
    E --> F[Fetch Nodes]
    F --> G[Resolve Weight Function]
    G --> H[Build Graph]
    H --> I[Compute Potential Matches]
    I --> J[Enqueue Matches]
    J --> K[Drain to MapDB]
    K --> L[Persist to PostgreSQL]
    L --> M[Mark Nodes Processed]
    M --> N[Flush Participation History]
    N --> O[Finalize Cycle]
```

### **Detailed Flow**
1. **Scheduled Cycle**:
    - Triggered by cron schedule
    - Enumerates active domains and groups
    - Acquires semaphores for domain/group concurrency control
2. **Per-Group Job**:
    - Fetches node IDs and nodes
    - Resolves weight function based on metadata
    - Builds graph (symmetric or bipartite)
    - Computes potential matches
3. **Persistence**:
    - Enqueues matches for draining
    - Drains to MapDB for staging
    - Persists to PostgreSQL
4. **Finalization**:
    - Marks nodes as processed
    - Flushes participation history
    - Finalizes cycle and cleans up

---

## **4) Components and Responsibilities**

### **Component Responsibility Diagram**
```mermaid
classDiagram
    class PotentialMatchesCreationScheduler {
        +Scheduled entrypoint
        +Builds task list
        +Triggers JobExecutor
    }

    class PotentialMatchesCreationJobExecutor {
        +Paged per-group processing
        +Retry/backoff
        +Limits concurrent pages
    }

    class PotentialMatchServiceImpl {
        +Fetch node IDs/nodes
        +Derive weight function
        +Drive GraphPreProcessor
    }

    class NodeFetchService {
        +Async, transactional fetch
        +Partitioned fetch of nodes
        +Marks nodes processed
    }

    class WeightFunctionResolver {
        +Chooses metadata-based weight function
        +Registers configurable weight function
    }

    class GraphPreProcessor {
        +Selects strategy: SYMMETRIC/BIPARTITE/AUTO
        +Global build concurrency
        +Deadline watchdog
    }

    class SymmetricGraphBuilder {
        +Index nodes
        +Chunk computation with backpressure
        +Retry on chunks
        +Stream edges for final save
    }

    class BipartiteGraphBuilder {
        +Cartesian chunking across partitions
        +Process chunk matches
        +Persist chunk results to MapDB
        +Rebuild graph from MapDB
    }

    class PotentialMatchComputationProcessorImp {
        +Per-group queue manager
        +Enqueue/de-duplicate
        +Dynamic flush
        +Drain to MapDB and PostgreSQL
    }

    class QueueManagerFactory {
        +Bounded per-group queues
        +Periodic/boosted/blocking flush
        +TTL eviction
    }

    class GraphStore {
        +Transient edges in MapDB
        +Batched writes with commit
        +Stream by group
        +Cleanup
    }

    class PotentialMatchSaver {
        +Async PostgreSQL persistence
        +Temp table + COPY + upsert
        +Deletion and counting
    }
```

### **Component Responsibilities**
| **Component** | **Responsibility** |
|---------------|--------------------|
| PotentialMatchesCreationScheduler | Scheduled entrypoint, task list building, JobExecutor trigger |
| PotentialMatchesCreationJobExecutor | Paged per-group processing with retry/backoff |
| PotentialMatchServiceImpl | Node fetching, weight function resolution, graph preprocessing |
| NodeFetchService | Async, transactional node fetching and marking |
| WeightFunctionResolver | Metadata-based weight function selection |
| GraphPreProcessor | Strategy selection, global build concurrency |
| SymmetricGraphBuilder | Node indexing, chunked computation, edge streaming |
| BipartiteGraphBuilder | Partition chunking, match processing, MapDB persistence |
| PotentialMatchComputationProcessorImp | Queue management, flushing, PostgreSQL persistence |
| QueueManagerFactory | Bounded queue management, flush orchestration |
| GraphStore | Transient edge storage, batched writes, streaming, cleanup |
| PotentialMatchSaver | Async PostgreSQL persistence with upsert |

---

## **5) Concurrency and Backpressure**

### **Concurrency Model Diagram**
```mermaid
graph TD
    subgraph Semaphores
        S1[domainSemaphore]
        S2[groupSemaphore]
        S3[pageSemaphore]
        S4[buildSemaphore]
        S5[computeSemaphore]
        S6[mappingSemaphore]
        S7[saveSemaphore]
        S8[storageSemaphore]
    end

    subgraph Executors
        E1[matchCreationExecutorService]
        E2[ioExecutorService]
        E3[graphExecutorService]
        E4[persistenceExecutor]
        E5[queueFlushExecutor]
        E6[watchdogExecutor]
    end

    S1 --> E1
    S2 --> E1
    S3 --> E1
    S4 --> E3
    S5 --> E3
    S6 --> E3
    S7 --> E4
    S8 --> E4
```

### **Backpressure Mechanisms**
- **Bounded Queues**: Capacity limits to prevent memory overload
- **Dynamic Flush Intervals**: Adjust based on queue fill ratio
- **Boosted Drains**: Triggered at fill ratio thresholds
- **Drops with Metrics**: Track and alert on queue overflows

### **Concurrency Controls**
- **Semaphores**: Control access to domains, groups, pages, builds, computations, mappings, saves
- **Executors**: Separate pools for orchestration, I/O, graph building, persistence, queue flushing, watchdog
- **Acquisition Order**: Domain → Group → Page → Build/Compute/Mapping → Save

---

## **6) Data and Persistence**

### **Data Model Diagram**
```mermaid
erDiagram
    NODE {
        uuid id PK
        string referenceId
        string type
        timestamp createdAt
        jsonb metaData
    }

    POTENTIAL_MATCH {
        string referenceId
        string matchedReferenceId
        double compatibilityScore
        uuid groupId
        uuid domainId
    }

    GRAPH_RECORDS_POTENTIAL_MATCH {
        string referenceId
        string matchedReferenceId
        double compatibilityScore
        uuid groupId
        uuid domainId
    }

    POTENTIAL_MATCH_ENTITY {
        uuid id PK
        string referenceId
        string matchedReferenceId
        double compatibilityScore
        uuid groupId
        uuid domainId
        string processingCycleId
        timestamp matchedAt
    }

    NODE ||--o{ POTENTIAL_MATCH : "has"
    POTENTIAL_MATCH ||--|{ GRAPH_RECORDS_POTENTIAL_MATCH : "transforms to"
    GRAPH_RECORDS_POTENTIAL_MATCH ||--|{ POTENTIAL_MATCH_ENTITY : "persists as"
```

### **Persistence Layers**
- **MapDB (GraphStore)**:
    - Key: `groupId:chunkIndex:referenceId:matchedReferenceId` → serialized `PotentialMatch`
    - Batched persist with commit
    - Streaming and cleanup by group
- **PostgreSQL (PotentialMatchStorageProcessor)**:
    - COPY to temp table + upsert into `public.potential_matches`
    - Upsert key: `(group_id, reference_id, matched_reference_id)`
    - Incremental and finalization save paths

---

## **7) Resilience and Error Handling**

### **Resilience Mechanisms Diagram**
```mermaid
graph TD
    A[Error Handling] --> B[Timeouts]
    A --> C[Retries & Backoff]
    A --> D[Circuit Breakers]
    A --> E[Fallbacks]

    B --> F[Semaphore acquisitions]
    B --> G[Async tasks]
    B --> H[Chunk processing]
    B --> I[Flushes]
    B --> J[MapDB commits]
    B --> K[DB saves]

    C --> L[Page processing]
    C --> M[Symmetric chunk processing]
    C --> N[Storage layer]

    D --> O[BipartiteGraphBuilder]

    E --> P[PotentialMatchComputationProcessor]
    E --> Q[GraphStore]
```

### **Error Handling Strategies**
- **Timeouts**: Applied to semaphore acquisitions, async tasks, chunk processing, flushes, MapDB commits, DB saves
- **Retries & Backoff**:
    - Page processing (configurable)
    - Symmetric chunk processing (exponential backoff)
    - Storage layer (`@Retryable` for batch saves/deletes)
- **Circuit Breakers/Fallbacks**:
    - BipartiteGraphBuilder (Resilience4j `@Retry`/`@CircuitBreaker` with fallback)
    - PotentialMatchComputationProcessor fallbacks for chunk and pending save
    - GraphStore fallbacks for persist/stream/clean
- **Shutdown**: Flush queues, remove instances, cleanup GraphStore, orderly executor termination

---

## **8) Observability**

### **Observability Diagram**
```mermaid
graph TD
    A[Metrics] --> B[Timers]
    A --> C[Counters]
    A --> D[Gauges]

    B --> E[batch_matches_total_duration]
    B --> F[matching_duration]
    B --> G[graph_preprocessor_duration]
    B --> H[storage_processor_duration]

    C --> I[nodes_processed_total]
    C --> J[matches_generated_total]
    C --> K[matching_errors_total]
    C --> L[queue_drain_warnings_total]

    D --> M[adjacency_map_current_size]
    D --> N[system_cpu_usage]
    D --> O[executor_queue_length]
```

### **Key Metrics**
- **Timers**: End-to-end durations, graph build durations, storage durations
- **Counters**: Nodes processed, matches generated, errors, queue warnings
- **Gauges**: Map sizes, CPU usage, executor queue lengths

### **Logging**
- **Tags**: `groupId`, `domainId`, `cycleId`, `page/chunk`
- **Levels**: Warnings for backpressure, timeouts, partial pages

---

## **9) Configuration & Tuning**

### **Configuration Matrix**
| **Category** | **Key** | **Default** | **Effect** |
|--------------|---------|-------------|------------|
| Scheduling | match.save.delay | 600000 | Fixed delay between cycles (ms) |
| Concurrency | match.max-concurrent-domains | 2 | Domain-level parallelism |
| Paging | match.batch-limit | 500 | Nodes per page request |
| Retries | match.max-retries | 3 | Page retry count |
| Graph Build | graph.max-concurrent-builds | 2 | Build parallelism |
| Queue & Flush | match.queue.capacity | 500k..1M | Bounded queue size |
| Storage | matches.save.batch-size | 5000 | DB batch size |
| MapDB | mapdb.path | d:/web_dev/... | DB file path |

### **Tuning Guidance**
- Increase `maxConcurrentDomains`, pool sizes, and queue capacity to scale throughput
- Adjust `chunkSize`, `node-fetch batch-size`, and commit threads to match I/O bandwidth
- Use flush thresholds and boosted drains to manage bursty loads

---

## **10) Scaling & Capacity Planning**

### **Scaling Diagram**
```mermaid
graph TD
    A[Horizontal Scaling] --> B[Domain/Group Concurrency]
    A --> C[Executor Pools]

    B --> D[maxConcurrentDomains]
    B --> E[groupSemaphore]

    C --> F[matchCreationExecutorService]
    C --> G[ioExecutorService]
    C --> H[graphExecutorService]
```

### **Capacity Planning**
- **Horizontal Scaling**: Via domain/group concurrency and executor pools
- **Backpressure**: Ensures bounded memory; queues provide smoothing for bursty producers
- **MapDB**: SSD-backed local paths recommended for low-latency persistence
- **Monitoring**: MapDB commit latency and queue flush throughput are critical

---

## **11) Security & Data Integrity**

### **Security Diagram**
```mermaid
graph TD
    A[Security] --> B[Data Integrity]
    A --> C[Access Control]

    B --> D[Upsert Semantics]
    B --> E[MapDB Transient Storage]

    D --> F[Prevents Duplicates]
    E --> G[Per-Group Cleanup]
```

### **Key Measures**
- **Upsert Semantics**: Prevents duplicates in PostgreSQL on `(group_id, reference_id, matched_reference_id)`
- **MapDB**: Transient and per-group cleaned after finalization
- **Credentials**: Managed via Hikari configuration (externalized)
- **Idempotency**: Finalization re-reads edges for final write safely

---

## **12) Trade-offs & Known Considerations**

### **Trade-offs Diagram**
```mermaid
graph TD
    A[Trade-offs] --> B[Two-Stage Persistence]
    A --> C[Blocking Waits]
    A --> D[MetadataEncoder Cache]

    B --> E[Adds I/O]
    B --> F[Operational Decoupling]

    C --> G[Requires Executor Sizing]
    C --> H[Prevents Starvation]

    D --> I[Shared Cache Risk]
    D --> J[Per-Batch Localization]
```

### **Considerations**
- **Two-Stage Persistence**: Adds I/O but provides backpressure and operational decoupling
- **Blocking Waits**: Requires adequate executor sizing to prevent starvation
- **MetadataEncoder Cache**: Batch-global cache reset not ideal if shared; prefer per-batch or strategy-local instances
- **Double-Processing Risk**: Verify dedupe/desired behavior in symmetric pipeline
- **Naming Consistency**: Minor log/metric duplication in GraphStore can be polished

---

## **13) Deployment & Operational Notes**

### **Deployment Diagram**
```mermaid
graph TD
    A[Prerequisites] --> B[PostgreSQL]
    A --> C[Local Disk for MapDB]

    B --> D[Write Throughput]
    C --> E[Write Permissions]
    C --> F[Adequate Space]
```

### **Operational Notes**
- **Prerequisites**: PostgreSQL reachable with sufficient write throughput; local disk path for MapDB
- **Shutdown**: Module flushes queues, removes queue managers, cleans GraphStore per group, closes executors
- **Portability**: Ensure `mapdb.path` configured appropriately for target OS/container

---

## **14) Sequence Flows**

### **Symmetric Strategy Flow**
```mermaid
sequenceDiagram
    participant Scheduler
    participant JobExecutor
    participant PotentialMatchService
    participant NodeFetchService
    participant GraphPreProcessor
    participant SymmetricGraphBuilder
    participant QueueManager
    participant GraphStore
    participant PotentialMatchSaver

    Scheduler->>JobExecutor: Trigger per-group job
    JobExecutor->>PotentialMatchService: Process group
    PotentialMatchService->>NodeFetchService: Fetch node IDs/nodes
    NodeFetchService-->>PotentialMatchService: Nodes
    PotentialMatchService->>GraphPreProcessor: Build graph
    GraphPreProcessor->>SymmetricGraphBuilder: Index nodes, process chunks
    SymmetricGraphBuilder->>QueueManager: Enqueue matches
    QueueManager->>GraphStore: Drain to MapDB
    QueueManager->>PotentialMatchSaver: Persist to PostgreSQL
    PotentialMatchSaver->>NodeFetchService: Mark nodes processed
```

### **Bipartite Strategy Flow**
```mermaid
sequenceDiagram
    participant GraphPreProcessor
    participant BipartiteGraphBuilder
    participant GraphStore
    participant PotentialMatchSaver

    GraphPreProcessor->>BipartiteGraphBuilder: Process chunks
    BipartiteGraphBuilder->>GraphStore: Persist chunk results
    BipartiteGraphBuilder->>GraphStore: Stream edges, rebuild graph
    GraphStore->>PotentialMatchSaver: Finalize to PostgreSQL
    GraphStore->>GraphStore: Cleanup
```

---

## **15) Glossary**

| **Term** | **Definition** |
|----------|----------------|
| **Domain/Group** | Logical segmentation of nodes for matching |
| **CycleId** | Unique identifier for a scheduled processing cycle |
| **MatchType** | SYMMETRIC (same-type nodes) vs BIPARTITE (left/right partitions) |
| **LSH** | Locality Sensitive Hashing for candidate pair narrowing |
| **MapDB** | Embedded key/value store for transient graph edge staging |

---

This completes the High-Level Design (HLD) with comprehensive diagrams for the **Scheduled Potential Match Creation** system.