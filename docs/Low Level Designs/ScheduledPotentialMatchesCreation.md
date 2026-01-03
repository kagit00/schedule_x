# Potential Matches Creation System - Low-Level Design Document
---
## Table of Contents
1. [System Overview](#1-system-overview)
2. [Architecture Design](#2-architecture-design)
3. [Component Design](#3-component-design)
4. [Data Flow Architecture](#4-data-flow-architecture)
5. [Graph Processing Engine](#5-graph-processing-engine)
6. [Storage Architecture](#6-storage-architecture)
7. [Queue Management System](#7-queue-management-system)
8. [Error Handling & Resilience](#8-error-handling--resilience)
---
## 1. System Overview
### 1.1 Purpose
This document describes a low-level reference design for a batch-oriented potential-match computation system, focusing on algorithmic structure and data flow rather than production deployment, tuning, or observed performance.

The **Potential Matches Creation System** is a batch-oriented graph processing design for computing compatibility relationships between entities using multiple edge-building strategies.

### 1.2 Key Capabilities
```mermaid
flowchart TB
    ROOT["Potential Matches<br/>System"]
    ROOT --> S["Scheduling"]
    ROOT --> G["Graph Building"]
    ROOT --> E["Edge Processing"]
    ROOT --> P["Persistence"]
    ROOT --> R["Resilience"]
    S --> S1["Cron-based Execution"]
    S --> S2["Cursor Pagination"]
    S --> S3["Incremental Processing"]
    G --> G1["Symmetric Graphs"]
    G --> G2["Bipartite Graphs"]
    G --> G3["LSH Indexing"]
    G --> G4["Metadata Weighting"]
    E --> E1["LMDB Streaming"]
    E --> E2["Batch Computation"]
    E --> E3["Queue Management"]
    E --> E4["Disk Spillover"]
    P --> P1["PostgreSQL COPY"]
    P --> P2["Advisory Locks"]
    P --> P3["Final Merging"]
    P --> P4["Duplicate Handling"]
    R --> R1["Retry Logic"]
    R --> R2["Semaphore Control"]
    R --> R3["Backpressure"]
    R --> R4["Graceful Shutdown"]
    style ROOT fill:#ECEFF1
    style S fill:#E3F2FD
    style G fill:#E8F5E9
    style E fill:#FFFDE7
    style P fill:#FCE4EC
    style R fill:#EDE7F6
```
---
## 2. Architecture Design
### 2.1 Layered Architecture
```mermaid
graph TB
    subgraph "Presentation Layer"
        A1[Scheduled Trigger<br/>@Scheduled Cron]
        A2[Manual Trigger API<br/>Future]
    end
   
    subgraph "Orchestration Layer"
        B1[PotentialMatchesCreationScheduler]
        B2[PotentialMatchesCreationJobExecutor]
        B3[MatchesCreationFinalizer]
    end
   
    subgraph "Service Layer"
        C1[PotentialMatchService]
        C2[NodeFetchService]
        C3[WeightFunctionResolver]
    end
   
    subgraph "Graph Processing Layer"
        D1[GraphPreProcessor]
        D2[SymmetricGraphBuilder]
        D3[BipartiteGraphBuilder]
        D4[EdgeBuildingStrategy Factory]
    end
   
    subgraph "Computation Layer"
        E1[PotentialMatchComputationProcessor]
        E2[QueueManager]
        E3[EdgeProcessor]
    end
   
    subgraph "Storage Layer"
        F1[GraphStore]
        F2[EdgePersistence]
        F3[PotentialMatchSaver]
        F4[StorageProcessor]
    end
   
    subgraph "Infrastructure Layer"
        G1[(LMDB<br/>Edge Cache)]
        G2[(PostgreSQL<br/>Match Results)]
        G3[Thread Pools]
        G4[Semaphore Control]
    end
   
    A1 --> B1
    B1 --> B2
    B2 --> C1
    C1 --> C2
    C1 --> D1
    D1 --> D2
    D1 --> D3
    D2 --> E1
    E1 --> E2
    E1 --> F1
    F1 --> F2
    F2 --> G1
    E1 --> F3
    F3 --> F4
    F4 --> G2
   
    B1 -.-> G4
    E1 -.-> G3
   
    style A1 fill:#4CAF50
    style B1 fill:#2196F3
    style C1 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#F44336
    style F1 fill:#00BCD4
    style G1 fill:#795548
```
### 2.3 Component Interaction Overview
```mermaid
graph TB
    subgraph "Phase 1: Initialization"
        P1A[Scheduler Trigger] --> P1B[Get Tasks to Process]
        P1B --> P1C[Acquire Domain Semaphore]
    end
   
    subgraph "Phase 2: Node Fetching"
        P2A[Cursor-based Pagination] --> P2B[Fetch Node Batch]
        P2B --> P2C[Hydrate Node Metadata]
        P2C --> P2D[Mark as Processed]
    end
   
    subgraph "Phase 3: Graph Building"
        P3A[Determine Match Type] --> P3B{Symmetric?}
        P3B -->|Yes| P3C[Symmetric Builder]
        P3B -->|No| P3D[Bipartite Builder]
        P3C --> P3E[Chunk into batches]
        P3D --> P3E
        P3E --> P3F[Parallel Edge Computation]
    end
   
    subgraph "Phase 4: Edge Processing"
        P4A[PotentialMatch Objects] --> P4B[Enqueue to QueueManager]
        P4B --> P4C{Queue Full?}
        P4C -->|Yes| P4D[Spill to Disk]
        P4C -->|No| P4E[Keep in Memory]
        P4D --> P4F[Flush Batch]
        P4E --> P4F
    end
   
    subgraph "Phase 5: Dual Persistence"
        P5A[Batch Ready] --> P5B[Save to LMDB]
        P5A --> P5C[Save to PostgreSQL]
        P5B --> P5D[UnifiedWriteOrchestrator]
        P5C --> P5E[StorageProcessor COPY]
    end
   
    subgraph "Phase 6: Finalization"
        P6A[Drain Pending Queue] --> P6B[Final Save Stream]
        P6B --> P6C[LMDB → PostgreSQL]
        P6C --> P6D[Update Job Status]
        P6D --> P6E[Cleanup & Release]
    end
   
    P1C --> P2A
    P2D --> P3A
    P3F --> P4A
    P4F --> P5A
    P5E --> P6A
   
    style P1A fill:#E8F5E9
    style P2A fill:#E3F2FD
    style P3A fill:#FFF9C4
    style P4A fill:#F3E5F5
    style P5A fill:#FFEBEE
    style P6A fill:#E0F2F1
```
---
## 3. Component Design
### 3.1 Scheduler Component
```mermaid
classDiagram
    class PotentialMatchesCreationScheduler {
        -DomainService domainService
        -MatchingGroupRepository matchingGroupRepository
        -PotentialMatchesCreationJobExecutor jobExecutor
        -PotentialMatchComputationProcessor processor
        -Semaphore domainSemaphore
        -ConcurrentMap~UUID,CompletableFuture~ groupLocks
        -ExecutorService batchExecutor
        +processAllDomainsScheduled() void
        -processGroupTask(GroupTaskRequest) CompletableFuture
        -executeGroupTaskChain(GroupTaskRequest) CompletableFuture
        -handleCycleCompletion(String, List, Throwable) void
        -acquireSemaphoreAsync(Semaphore, UUID) CompletableFuture
        -cleanupIdleGroupLocks() void
    }
   
    class GroupTaskRequest {
        +Domain domain
        +UUID groupId
        +String cycleId
    }
   
    class Semaphore {
        <<Java Concurrency>>
        +tryAcquire(long, TimeUnit) boolean
        +release() void
    }
   
    class CompletableFuture~Void~ {
        <<Java Async>>
        +thenCompose(Function) CompletableFuture
        +whenCompleteAsync(BiConsumer, Executor) CompletableFuture
        +exceptionally(Function) CompletableFuture
    }
   
    PotentialMatchesCreationScheduler --> "1" Semaphore : domainSemaphore
    PotentialMatchesCreationScheduler --> "*" GroupTaskRequest : processes
    PotentialMatchesCreationScheduler ..> CompletableFuture : uses
```
**Responsibility Matrix**:
| Responsibility | Implementation | Rationale |
|----------------|----------------|-----------|
| **Job Scheduling** | `@Scheduled` annotation with cron | Spring-managed, reliable timing |
| **Concurrency Control** | Semaphore (configurable permits) | Limit concurrent domain processing |
| **Task Distribution** | `CompletableFuture` chains | Async, non-blocking execution |
| **Group Serialization** | `ConcurrentMap` of futures | Prevent concurrent processing of same group |
| **Resource Cleanup** | `cleanupIdleGroupLocks()` | Remove completed futures from map |
**Processing Flow**:
```mermaid
sequenceDiagram
    participant Cron as Spring Scheduler
    participant Sch as Scheduler
    participant Sem as Domain Semaphore
    participant Exec as Job Executor
    participant Proc as Computation Processor
   
    Cron->>Sch: Trigger
    Sch->>Sch: Get Active Domains & Groups
   
    loop For Each Group
        Sch->>Sch: Check groupLocks map
        alt Group Already Processing
            Sch->>Sch: Chain to existing future
        else Group Idle
            Sch->>Sem: tryAcquire()
            alt Semaphore Acquired
                Sem-->>Sch: Success
                Sch->>Exec: processGroup(groupId, domainId, cycleId)
               
                Exec->>Exec: Fetch nodes in batches
                Exec->>Exec: Process batch with retries
                Exec-->>Sch: Batch complete
               
                Sch->>Proc: savePendingMatchesAsync()
                Proc-->>Sch: Queue drained
               
                Sch->>Proc: saveFinalMatches()
                Proc-->>Sch: Final save complete
               
                Sch->>Proc: cleanup(groupId)
                Sch->>Sem: release()
            else Timeout
                Sem-->>Sch: TimeoutException
                Sch->>Sch: Log error, skip group
            end
        end
    end
   
    Sch->>Sch: cleanupIdleGroupLocks()
    Sch->>Cron: Cycle complete
```
### 3.2 Job Executor Component
```mermaid
classDiagram
    class PotentialMatchesCreationJobExecutor {
        -PotentialMatchService potentialMatchService
        -NodeFetchService nodeFetchService
        -MeterRegistry meterRegistry
        -ExecutorService batchExecutor
        +processGroup(UUID, UUID, String) CompletableFuture
        -processGroupRecursive(UUID, UUID, String, int) CompletableFuture
        -processPageWithRetries(UUID, UUID, String, List) CompletableFuture
        -processPageAttempt(UUID, UUID, String, List, int) CompletableFuture
    }
   
    class NodeFetchService {
        -NodeRepository nodeRepository
        -NodesCursorRepository nodesCursorRepository
        +fetchNodeIdsByCursor(UUID, UUID, int, String) CompletableFuture~CursorPage~
        +fetchNodesInBatchesAsync(List~UUID~, UUID, LocalDateTime) CompletableFuture~List~NodeDTO~~
        +markNodesAsProcessed(List~UUID~, UUID) void
        +persistCursor(UUID, UUID, OffsetDateTime, UUID) void
    }
   
    class CursorPage {
        +List~UUID~ ids
        +boolean hasMore
        +LocalDateTime lastCreatedAt
        +UUID lastId
    }
   
    class PotentialMatchService {
        <<interface>>
        +processNodeBatch(List~UUID~, MatchingRequest) CompletableFuture~NodesCount~
    }
   
    PotentialMatchesCreationJobExecutor --> NodeFetchService
    PotentialMatchesCreationJobExecutor --> PotentialMatchService
    NodeFetchService ..> CursorPage : returns
```
**Cursor-Based Pagination Strategy**:
```mermaid
flowchart TB
    A[Start] --> B[Load Cursor Position<br/>createdAt, nodeId]
    B --> C{Cursor Exists?}
    C -->|No| D[Fetch from beginning<br/>WHERE processed=false]
    C -->|Yes| E[Fetch with overlap<br/>createdAt > cursor OR<br/>createdAt = cursor AND id > cursorId]
   
    D --> F[Fetch configurable LIMIT]
    E --> F
   
    F --> G{Result Size}
    G -->|Small| H[Process all<br/>hasMore=false]
    G -->|Large| I[Process subset<br/>hasMore=true]
   
    H --> J[Update Cursor<br/>to last item]
    I --> K[Update Cursor<br/>to processed boundary]
   
    J --> L[Mark Nodes Processed]
    K --> L
   
    L --> M{hasMore?}
    M -->|Yes| B
    M -->|No| N[End]
   
    style A fill:#4CAF50
    style F fill:#2196F3
    style L fill:#FF9800
    style N fill:#4CAF50
```
**Why Overlapping Window?**
- **Prevents Data Loss**: Concurrent inserts with same `createdAt` won't be skipped
- **Idempotency**: Duplicate processing handled by `processed=false` filter
- **Safety Margin**: Configurable overlap ensures completeness
### 3.3 Service Layer Component
```mermaid
classDiagram
    class PotentialMatchServiceImpl {
        -NodeFetchService nodeFetchService
        -WeightFunctionResolver weightFunctionResolver
        -GraphPreProcessor graphPreProcessor
        -Semaphore dbFetchSemaphore
        -ConcurrentLinkedQueue~MatchParticipationHistory~ historyBuffer
        +processNodeBatch(List~UUID~, MatchingRequest) CompletableFuture~NodesCount~
        -fetchNodesInSubBatches(List~UUID~, UUID, LocalDateTime) CompletableFuture~List~NodeDTO~~
        -processGraphAndMatches(List~NodeDTO~, MatchingRequest, UUID, String) CompletableFuture
        -bufferMatchParticipationHistory(List~NodeDTO~, UUID, UUID, String) void
        -flushHistoryIfNeeded() void
        -acquireDbSemaphore() CompletableFuture
    }
   
    class WeightFunctionResolver {
        -NodeRepository nodeRepository
        +resolveWeightFunctionKey(UUID) String
    }
   
    class GraphPreProcessor {
        -SymmetricGraphBuilderService symmetricGraphBuilder
        -BipartiteGraphBuilderService bipartiteGraphBuilder
        -PartitionStrategy partitionStrategy
        -Semaphore buildSemaphore
        +buildGraph(List~NodeDTO~, MatchingRequest) CompletableFuture~GraphResult~
        +inferMatchType(List~NodeDTO~, List~NodeDTO~) MatchType
        +determineMatchTypeFromExistingData(UUID, UUID, String) MatchType
        -acquireAndBuildAsync(UUID, Supplier) CompletableFuture
    }
   
    PotentialMatchServiceImpl --> WeightFunctionResolver
    PotentialMatchServiceImpl --> GraphPreProcessor
```
**Node Fetching Optimization**:
```mermaid
graph LR
    A[Node IDs] -->|Partition| B1[Sub-batch]
   
    B1 -->|Acquire Semaphore| C1[DB Query]
   
    C1 -->|Hydrate| D1[NodeDTO List]
   
    D1 -->|Release Semaphore| E[Merge Results]
   
    E --> F[Complete List]
   
    style A fill:#E3F2FD
    style C1 fill:#FFF9C4
    style E fill:#C8E6C9
    style F fill:#4CAF50
```
**Semaphore Strategy**:
- **Purpose**: Limit concurrent database queries to prevent connection pool exhaustion
- **Permits**: Configurable
- **Timeout**: Configurable
- **Benefits**: Prevents thundering herd, maintains predictable load
---
## 4. Data Flow Architecture
### 4.1 End-to-End Data Flow
```mermaid
flowchart TB
    subgraph G["3. Graph Building"]
        D1[Determine MatchType] --> D2{Symmetric?}
        D2 -->|Yes| D3[Symmetric Builder - LSH Metadata]
        D2 -->|No| D4[Bipartite Builder]
        D4 --> D5[Partition Nodes left right]
        D5 --> D6[Chunk into configurable batches]
        D6 --> D7[Parallel Chunk Pairs left i x right j]
        D7 --> D8[Compute Metadata Similarity]
        D8 --> D9[Generate PotentialMatches]
    end
    D9 --> E1[Enqueue to QueueManager]
    E1 --> E2{Queue Full?}
    E2 -->|Yes| E3[Spill to Disk]
    E2 -->|No| E4[Keep in Memory]
    E3 --> E5[Flush Batch]
    E4 --> E5[Flush Batch]
    E5 --> F1[LMDB Write]
    E5 --> F2[PostgreSQL Write]
    style D4 fill:#F48FB1
    style D7 fill:#CE93D8
    style D8 fill:#B39DDB
```
### 4.2 Detailed Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler
    participant EXE as JobExecutor
    participant FETCH as NodeFetchService
    participant PG as PostgreSQL
    participant SVC as MatchService
    participant PRE as GraphPreprocessor
    participant BUILDER as GraphBuilder
    participant QUEUE as QueueManager
    participant PROC as ComputationProcessor
    participant LMDB as LMDBStore
    participant STORE as StorageProcessor
    SCH->>SCH: Scheduled trigger
    SCH->>SCH: Acquire domain semaphore
    SCH->>EXE: processGroup(groupId, domainId, cycleId)
    loop Until empty streak reached
        EXE->>FETCH: fetchNodeIdsByCursor(groupId, limit)
        FETCH->>PG: Select unprocessed node ids
        PG-->>FETCH: Cursor page
        FETCH-->>EXE: Node id batch
        alt Batch not empty
            EXE->>SVC: processNodeBatch(nodeIds)
            SVC->>FETCH: fetchNodesAsync(nodeIds)
            FETCH->>PG: Select nodes with metadata
            PG-->>FETCH: Node records
            FETCH-->>SVC: Node DTOs
        end
    end
    SVC->>PRE: buildGraph(nodes)
    PRE->>PRE: determine match type
    PRE->>BUILDER: build graph
    BUILDER->>BUILDER: index nodes
    BUILDER->>BUILDER: partition into chunks
    par Parallel workers
        BUILDER->>BUILDER: process chunk
        BUILDER->>BUILDER: process chunk
        BUILDER->>BUILDER: process chunk
    end
    BUILDER-->>PRE: chunk match results
    loop For each chunk result
        PRE->>PROC: processChunkMatches
        PROC->>QUEUE: enqueue match
        alt Queue exceeds threshold
            QUEUE->>QUEUE: flush batch
            QUEUE-->>PROC: match batch
        end
    end
    PROC->>LMDB: persist edges async
    PROC->>STORE: save potential matches
    par Concurrent persistence
        LMDB->>LMDB: batch write transaction
        STORE->>PG: COPY and upsert
    end
    SCH->>PROC: finalize processing
    loop Drain remaining queue
        PROC->>QUEUE: drain batch
        QUEUE-->>PROC: matches
        PROC->>STORE: save batch
    end
    PROC->>LMDB: stream final edges
    loop Stream edges
        LMDB-->>PROC: edge
        PROC->>STORE: flush batch
    end
    PROC->>QUEUE: cleanup group
    SCH->>SCH: Release semaphore
```
---
## 5. Graph Processing Engine
### 5.1 Graph Builder Architecture
```mermaid
classDiagram
    class SymmetricGraphBuilder {
        -SymmetricEdgeBuildingStrategyFactory strategyFactory
        -PotentialMatchComputationProcessor processor
        -ExecutorService computeExecutor
        -int chunkSize
        -int maxConcurrentWorkers
        -int matchBatchSize
        -int topK
        +build(List~NodeDTO~, MatchingRequest) CompletableFuture~GraphResult~
        -finalizeBuild(MatchingRequest, CompletableFuture) CompletableFuture
        -startConcurrentWorkers(...) void
        -runWorkerChain(...) CompletableFuture
    }
   
    class SymmetricEdgeBuildingStrategyFactory {
        -FlatEdgeBuildingStrategy flatEdgeBuildingStrategy
        -ObjectProvider~LSHIndex~ lshIndexProvider
        -MetadataEncoder encoder
        -EdgeProcessor edgeProcessor
        +createStrategy(String, List~NodeDTO~) SymmetricEdgeBuildingStrategy
    }
   
    class SymmetricEdgeBuildingStrategy {
        <<interface>>
        +indexNodes(List~NodeDTO~, int) CompletableFuture
        +processBatch(List, List, List, Set, MatchingRequest, Map) void
    }
   
    class FlatEdgeBuildingStrategy {
        +indexNodes(...) CompletableFuture
        +processBatch(...) void
    }
   
    class MetadataEdgeBuildingStrategy {
        -LSHIndex lshIndex
        -MetadataEncoder encoder
        -EdgeProcessor edgeProcessor
        +indexNodes(...) CompletableFuture
        +processBatch(...) void
    }
   
    class TaskIterator {
        -List~List~NodeDTO~~ allChunks
        -int totalTasks
        +getTaskByIndex(int) ChunkTask
        +getTotalTasks() int
    }
   
    SymmetricGraphBuilder --> SymmetricEdgeBuildingStrategyFactory
    SymmetricEdgeBuildingStrategyFactory ..> SymmetricEdgeBuildingStrategy : creates
    SymmetricEdgeBuildingStrategy <|.. FlatEdgeBuildingStrategy
    SymmetricEdgeBuildingStrategy <|.. MetadataEdgeBuildingStrategy
    SymmetricGraphBuilder ..> TaskIterator : uses
```
### 5.2 Strategy Selection Flow
```mermaid
flowchart TD
    A[WeightFunctionResolver] --> B[Query Node Metadata Keys]
    B --> C{Valid Keys Found?}
    C -->|No| D[Return 'flat']
    C -->|Yes| E[Generate Weight Key<br/>groupId-key1-key2-...]
   
    E --> F{Strategy Exists?}
    F -->|No| G[Create & Register<br/>ConfigurableMetadataWeightFunction]
    F -->|Yes| H[Return Existing Key]
   
    G --> I[StrategyFactory.createStrategy]
    H --> I
    D --> J[FlatEdgeBuildingStrategy]
   
    I --> K{Key == 'flat'?}
    K -->|Yes| J
    K -->|No| L[MetadataEdgeBuildingStrategy]
   
    L --> M[Create LSHIndex]
    L --> N[Setup MetadataEncoder]
    L --> O[Configure EdgeProcessor]
   
    style A fill:#4CAF50
    style E fill:#2196F3
    style J fill:#FF9800
    style L fill:#9C27B0
```
### 5.3 Symmetric Graph Building Process
```mermaid
sequenceDiagram
    autonumber
    participant Builder as SymmetricGraphBuilder
    participant Strategy as EdgeBuildingStrategy
    participant Iterator as TaskIterator
    participant Worker as Worker Thread
    participant Proc as ComputationProcessor
    participant Queue as QueueManager
   
    Builder->>Builder: Partition nodes into configurable chunks
    Builder->>Iterator: new TaskIterator(allChunks)
   
    Note over Iterator: Calculate cross-product tasks<br/>chunk1×chunk1, chunk1×chunk2, ...
   
    Builder->>Strategy: indexNodes(allNodes)
   
    alt LSH Strategy
        Strategy->>Strategy: Encode node metadata
        Strategy->>Strategy: Generate LSH signatures
        Strategy->>Strategy: Build bucket index
    else Flat Strategy
        Strategy->>Strategy: No indexing needed
    end
   
    Strategy-->>Builder: Indexing complete
   
    Builder->>Builder: Spawn concurrent workers
   
    par Worker 1
        Worker->>Iterator: getTaskByIndex(counter++)
        Iterator-->>Worker: ChunkTask(source, target)
        Worker->>Strategy: processBatch(source, target)
        Strategy-->>Worker: List<PotentialMatch>
        Worker->>Proc: processChunkMatches(matches)
        Proc->>Queue: enqueue(match)
        Worker->>Worker: Recursively get next task
    and Worker 2
        Worker->>Iterator: getTaskByIndex(counter++)
        Worker->>Strategy: processBatch(...)
        Worker->>Proc: processChunkMatches(...)
    and Worker N
        Worker->>Iterator: getTaskByIndex(counter++)
        Worker->>Strategy: processBatch(...)
        Worker->>Proc: processChunkMatches(...)
    end
   
    Note over Builder,Queue: All tasks complete
   
    Builder->>Proc: savePendingMatchesAsync()
    Proc->>Queue: drainBatch()
    Queue-->>Proc: Batch
    Proc->>Proc: Save to LMDB + PostgreSQL
   
    Builder->>Proc: saveFinalMatches()
    Builder->>Builder: Complete future
```
### 5.4 Bipartite Graph Building Process
Handles matching between two disjoint node sets (e.g., Patients vs Doctors). Uses brute-force chunked comparison (no LSH).
```mermaid
sequenceDiagram
    autonumber
    participant PreProc as GraphPreProcessor
    participant PartStrat as PartitionStrategy
    participant Builder as BipartiteGraphBuilder
    participant EdgeStrat as BipartiteEdgeBuildingStrategy
    participant Queue as QueueManager
    participant Store as GraphStore
    PreProc->>PartStrat: partition(nodes, key, leftValue, rightValue)
    PartStrat-->>PreProc: leftNodes, rightNodes
    alt MatchType == SYMMETRIC
        PreProc->>PreProc: Route to SymmetricBuilder
    else MatchType == BIPARTITE
        PreProc->>Builder: build(leftNodes, rightNodes, request)
       
        Builder->>Builder: Split left/right into configurable chunks
        Note over Builder: leftChunks = BatchUtils.partition(left, configurable)<br/>rightChunks = BatchUtils.partition(right, configurable)
        Builder->>Builder: Create task pairs (leftChunk[i] × rightChunk[j])
        Note over Builder: Total tasks = leftChunks.size() × rightChunks.size()
        loop For each chunk pair
            Builder->>EdgeStrat: processBatch(leftChunk, rightChunk)
            EdgeStrat->>EdgeStrat: Compute metadata similarity
            EdgeStrat-->>Builder: List<PotentialMatch>
            Builder->>Queue: enqueue(matches)
        end
        Builder->>Store: persistEdgesAsync(matches)
        Store->>Store: Save to LMDB + PostgreSQL (async)
        Builder-->>PreProc: GraphResult
    end
```
### 5.5 Bipartite Edge Computation Flow
Edge creation logic in BipartiteEdgeBuildingStrategy.processBatch():
```mermaid
flowchart TD
    A[Start: Left Batch & Right Batch] --> B[Loop over leftBatch]
    B --> C[Loop over rightBatch]
    C --> D[Calculate similarity score]
   
    D --> E{score > threshold?}
    E -->|Yes| F[Create PotentialMatch]
    E -->|No| C
   
    F --> G[Add to matches list]
    C -->|End right loop| B
    B -->|End left loop| H[Return matches]
   
    style D fill:#FFF9C4
    style F fill:#C8E6C9
```
**Similarity Calculation** (`MetadataCompatibilityCalculator`):
| Step | Logic |
|-------------------------------|----------------------------------------------------------------------|
| **1. Common Keys** | `commonKeys = meta1.keySet() ∩ meta2.keySet()` |
| **2. Per-Key Scoring** | |
| &nbsp;&nbsp;- Exact match | High score |
| &nbsp;&nbsp;- Numeric match | High score if within tolerance |
| &nbsp;&nbsp;- Multi-value | High score if shared item in comma-separated list |
| &nbsp;&nbsp;- Partial match | Moderate score if one value contains the other |
| &nbsp;&nbsp;- No match | Zero score |
| **3. Final Score** | Aggregated per-key scores |
| **4. Threshold** | Only create edge if score exceeds configurable threshold |
> 💡 **Why no LSH?**
> Bipartite graphs compare **distinct sets**. LSH is inefficient here because:
> 1. No need to reduce O(*m×n*) comparisons beyond chunking.
> 2. Metadata-based similarity is cheap to compute (string operations).
### 5.6 Cross-Product Task Calculation
**Formula**: For N chunks, total tasks = N × (N + 1) / 2 (symmetric) or leftChunks × rightChunks (bipartite).
---
## 6. Storage Architecture
### 6.1 Dual Storage Design
```mermaid
graph TB
    subgraph "Match Generation"
        A[PotentialMatch Objects]
    end
   
    subgraph "Write Path"
        A --> B{Queue Manager}
        B -->|In-Memory| C[Memory Queue<br/>Configurable Capacity]
        B -->|Overflow| D[Disk Spill<br/>Temp Files]
        C --> E[Flush Batch]
        D --> E
    end
   
    subgraph "Dual Persistence"
        E --> F[LMDB Write]
        E --> G[PostgreSQL Write]
    end
   
    subgraph "LMDB Storage"
        F --> H[UnifiedWriteOrchestrator]
        H --> I[Writer Thread Queue]
        I --> J[Batch Write Transaction]
        J --> K[(LMDB Files<br/>Edge Cache)]
    end
   
    subgraph "PostgreSQL Storage"
        G --> L[StorageProcessor]
        L --> M[Acquire Advisory Lock]
        M --> N[CREATE TEMP TABLE]
        N --> O[COPY Binary Protocol]
        O --> P[MERGE via INSERT ON CONFLICT]
        P --> Q[(PostgreSQL<br/>potential_matches)]
    end
   
    subgraph "Final Save Phase"
        K --> R[Stream Edges]
        R --> S[Convert to Entities]
        S --> T[Final Batch Save]
        T --> Q
    end
   
    style A fill:#4CAF50
    style E fill:#FF9800
    style K fill:#2196F3
    style Q fill:#9C27B0
    style T fill:#F44336
```
### 6.2 LMDB Key-Value Structure
```mermaid
graph LR
    subgraph "Edge Key Structure"
        K1[Group ID<br/>UUID] --> K2[Cycle Hash]
        K2 --> K3[Node Pair Hash<br/>Sorted]
    end
   
    subgraph "Edge Value Structure"
        V1[Score<br/>float] --> V2[Domain ID<br/>UUID]
        V2 --> V3[From Length<br/>int]
        V3 --> V4[From NodeHash<br/>UTF-8]
        V4 --> V5[To Length<br/>int]
        V5 --> V6[To NodeHash<br/>UTF-8]
    end
   
    K1 -.->|Maps to| V1
   
    style K1 fill:#E3F2FD
    style K2 fill:#E3F2FD
    style K3 fill:#E3F2FD
    style V1 fill:#FFF9C4
    style V2 fill:#FFF9C4
```
**Key Design Rationale**:
- **Grouping by groupId**: Efficient prefix scans for cleanup
- **Cycle Hash**: Isolates processing runs, enables concurrent cycles
- **Sorted Node Pair**: Ensures undirected edge deduplication (A→B == B→A)
- **Fixed-size prefix**: Fast prefix matching for streaming
### 6.3 UnifiedWriteOrchestrator Design
```mermaid
sequenceDiagram
    participant Client as Client Thread
    participant Queue as BlockingQueue<br/>Configurable Capacity
    participant Writer as Writer Thread
    participant LMDB as LMDB Env
   
    loop Concurrent Enqueue
        Client->>Queue: enqueueEdgeWrite(matches)
        Queue-->>Client: CompletableFuture
        Client->>Queue: enqueueLshWrite(buckets)
        Queue-->>Client: CompletableFuture
    end
   
    loop Writer Thread Loop
        Writer->>Queue: poll()
        Queue-->>Writer: WriteRequest
       
        Writer->>Writer: drainTo(batch)
        Writer->>Writer: Partition by type
       
        alt Edge Writes
            Writer->>Writer: Partition into batches
            loop For each batch
                Writer->>LMDB: txnWrite()
                Writer->>LMDB: dbi.put(key, value)
                Writer->>LMDB: txn.commit()
                Writer->>Client: future.complete()
            end
        end
       
        alt LSH Writes
            loop For each LSH request
                Writer->>LMDB: txnWrite()
                Writer->>LMDB: Merge bucket data
                Writer->>LMDB: txn.commit()
                Writer->>Client: future.complete()
            end
        end
    end
```
**Design Benefits**:
- **Single Writer Thread**: Eliminates LMDB write contention
- **Batching**: Groups multiple requests into single transaction
- **Async API**: Non-blocking enqueue with CompletableFuture
- **Bounded Queue**: Backpressure when queue full
### 6.4 PostgreSQL Storage Optimization
```mermaid
flowchart TB
    A[Batch Ready] --> B[Acquire advisory lock]
    B --> C[Begin transaction]
    C --> D[Set session parameters]
    D --> E[Create temp table]
    E --> F[Partition into chunks]
    F --> G1[COPY chunk to temp table]
    F --> G2[COPY chunk to temp table]
    F --> GN[COPY chunk to temp table]
    G1 --> H[Upsert into potential_matches]
    G2 --> H
    GN --> H
    H --> I[Commit transaction]
    I --> J[Release advisory lock]
```
**Performance Optimizations**:
| Technique | Benefit | Implementation |
|-----------|---------|----------------|
| **Binary COPY** | Faster bulk loading | PostgreSQL CopyManager |
| **Temp Tables** | Reduced overhead | Session-scoped TEMP TABLE |
| **Advisory Locks** | Prevent concurrent group writes | `pg_try_advisory_lock(hashtext(groupId))` |
| **Batch Merging** | Single UPSERT for all rows | INSERT ON CONFLICT DO UPDATE |
---
## 7. Queue Management System
### 7.1 QueueManager Architecture
```mermaid
classDiagram
    class QueueManagerImpl {
        -memoryQueue
        -memoryQueueSize
        -diskSpillManager
        -flushScheduler
        -capacity
        -flushIntervalSeconds
        -drainWarningThreshold
        -useDiskSpill
        +enqueue(match)
        +drainBatch(limit)
        +getQueueSize()
        +getDiskSpillSize()
    }
    class DiskSpillManager {
        -spillDirectory
        -spilledCount
        -spillFiles
        +spillBatch(batch)
        +readBatch(limit)
        +cleanup()
    }
    class QueueManagerFactory {
        -watchdogExecutor
        -meterRegistry
        +create(config)
    }
    class QueueConfig {
        +groupId
        +domainId
        +processingCycleId
        +capacity
        +flushIntervalSeconds
        +drainWarningThreshold
        +boostBatchFactor
        +maxFinalBatchSize
        +useDiskSpill
    }
    QueueManagerImpl --> DiskSpillManager : spills to
    QueueManagerFactory ..> QueueManagerImpl : creates
    QueueManagerFactory ..> QueueConfig : uses
```
### 7.2 Queue Operation Flow
```mermaid
flowchart TD
    Start([Start]) --> Enqueue[Enqueue match]
    Enqueue --> CheckCapacity[Check capacity]
    CheckCapacity -->|Below limit| InMemory[Store in memory]
    CheckCapacity -->|At limit| SpillToDisk[Spill to disk]
    InMemory --> PeriodicFlush[Periodic flush trigger]
    SpillToDisk --> DiskWrite[Write batch to disk]
    DiskWrite --> Enqueue
    PeriodicFlush --> DrainBatch[Drain batch]
    DrainBatch --> ReadMemory[Read from memory]
    ReadMemory -->|Empty| ReadDisk[Read from disk]
    ReadMemory -->|Has data| ProcessBatch[Process batch]
    ReadDisk --> ProcessBatch
    ProcessBatch --> SaveLMDB[Persist to LMDB]
    ProcessBatch --> SavePostgres[Persist to PostgreSQL]
    SaveLMDB --> Enqueue
    SavePostgres --> Enqueue
    Enqueue -->|All processed| Finalize[Finalize]
    Finalize --> Cleanup[Cleanup group]
```
### 7.3 Disk Spillover Mechanism
**Spill File Format**:
```
┌─────────────────┬─────────────────┬─────────────────┬───────┐
│ Magic Header │ Version │ Batch Size │ Data │
│ Fixed bytes │ Fixed bytes │ int │ ... │
└─────────────────┴─────────────────┴─────────────────┴───────┘
Data Section (repeated):
┌─────────────────┬─────────────────┬─────────────────┬───────┐
│ GroupId UUID │ DomainId UUID │ RefId Length │ RefId │
│ Fixed │ Fixed │ int │ Variable │
├─────────────────┼─────────────────┼─────────────────┼───────┤
│ MatchId Length │ MatchId │ Score │ │
│ int │ Variable │ float │ │
└─────────────────┴─────────────────┴─────────────────┴───────┘
```
---
## 8. Error Handling & Resilience
### 8.1 Retry Mechanism
```mermaid
flowchart TD
    A[Process Batch] --> B{Success?}
    B -->|Yes| C[Mark Nodes Processed]
    B -->|No| D{Retry Count < limit?}
   
    D -->|Yes| E[Exponential Backoff]
    E --> F[Wait]
    F --> A
   
    D -->|No| G[Log Error]
    G --> H[Skip Batch<br/>Continue with Next]
   
    C --> I[Persist Cursor]
    H --> I
    I --> J[Next Batch]
   
    style A fill:#2196F3
    style C fill:#4CAF50
    style G fill:#F44336
    style I fill:#FF9800
```
### 8.2 Backpressure Handling
```mermaid
sequenceDiagram
    participant Prod as Producer (Graph Builder)
    participant Queue as QueueManager
    participant Proc as Processor
    participant Sem as Storage Semaphore
   
    loop Fast Production
        Prod->>Queue: enqueue(match)
        Queue->>Queue: Check capacity
       
        alt Queue below threshold
            Queue-->>Prod: Accepted
        else Queue at threshold
            Queue->>Queue: Spill to disk
            Queue-->>Prod: Accepted (after spill)
        end
    end
   
    Note over Proc,Sem: Slower Consumption
   
    Proc->>Sem: tryAcquire()
    Sem-->>Proc: Wait (configurable permits)
   
    Proc->>Queue: drainBatch()
    Queue-->>Proc: Batch
   
    Proc->>Proc: Save to DB
    Proc->>Sem: release()
   
    alt Queue growing
        Queue->>Proc: Apply backpressure
        Proc->>Proc: Controlled delay
        Proc->>Proc: Continue draining
    end
```
### 8.3 Graceful Shutdown
```mermaid
stateDiagram-v2
    [*] --> Running: System Active
   
    Running --> PreDestroy: @PreDestroy Triggered
   
    PreDestroy --> FlushQueues: Flush all pending queues
   
    FlushQueues --> WaitDrain: savePendingMatchesAsync()
    WaitDrain --> DrainComplete: Timeout
    WaitDrain --> DrainTimeout: Timeout exceeded
   
    DrainComplete --> ShutdownExecutors: Graceful shutdown
    DrainTimeout --> ForceShutdownExecutors: Force shutdown
   
    ShutdownExecutors --> CloseConnections: Close DB connections
    ForceShutdownExecutors --> CloseConnections
   
    CloseConnections --> CloseLMDB: Close LMDB environment
   
    CloseLMDB --> [*]: Shutdown complete
```
**Shutdown Sequence**:
1. Set shutdown flag
2. Flush all QueueManagers (configurable timeout)
3. Shutdown executors gracefully (configurable timeout)
4. Force shutdown if needed
5. Close database connections
6. Close LMDB environment
7. Log completion metrics
---
## Appendix A: Database Schema
### A.1 Core Tables
```sql
-- Nodes Table
CREATE TABLE public.nodes (
    id UUID PRIMARY KEY,
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    reference_id VARCHAR(255) NOT NULL,
    type VARCHAR(50),
    metadata JSONB,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    CONSTRAINT uq_node_reference UNIQUE (group_id, domain_id, reference_id)
);
CREATE INDEX idx_nodes_group_domain ON nodes(group_id, domain_id);
CREATE INDEX idx_nodes_processed ON nodes(group_id, domain_id, processed) WHERE processed = false;
CREATE INDEX idx_nodes_cursor ON nodes(group_id, domain_id, created_at, id) WHERE processed = false;
-- Potential Matches Table
CREATE TABLE public.potential_matches (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    processing_cycle_id VARCHAR(255),
    reference_id VARCHAR(255) NOT NULL,
    matched_reference_id VARCHAR(255) NOT NULL,
    compatibility_score FLOAT NOT NULL,
    matched_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_match UNIQUE (group_id, reference_id, matched_reference_id)
);
CREATE INDEX idx_matches_group ON potential_matches(group_id, domain_id);
CREATE INDEX idx_matches_cycle ON potential_matches(processing_cycle_id);
CREATE INDEX idx_matches_reference ON potential_matches(reference_id);
-- Nodes Cursor Table
CREATE TABLE public.nodes_cursor (
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    cursor_created_at TIMESTAMP WITH TIME ZONE,
    cursor_id UUID,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (group_id, domain_id)
);
-- Match Participation History
CREATE TABLE public.match_participation_history (
    id BIGSERIAL PRIMARY KEY,
    node_id UUID NOT NULL,
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    processing_cycle_id VARCHAR(255),
    participated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_node FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);
CREATE INDEX idx_participation_node ON match_participation_history(node_id);
CREATE INDEX idx_participation_cycle ON match_participation_history(processing_cycle_id);
```