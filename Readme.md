# **GraphMatch Engine**
### *A Scalable, Fault-Tolerant Matchmaking System for High-Volume Node Similarity & Pairing*

---

## **Overview**
**GraphMatch Engine** is a **distributed, batch-oriented matchmaking platform** that processes millions of user nodes across domains and generates **high-quality potential and perfect matches** using **Locality-Sensitive Hashing (LSH)**, **metadata scoring**, and **optimal pairing algorithms**.

The system is **resilient**, **observable**, and **scalable**, delivering matches to clients via **file exports** and **Kafka streams**.

---

# **Detailed UML & Architecture Diagrams**

---

## **1. High-Level System Architecture**

```mermaid
graph TD
    subgraph "Ingestion"
        A[Kafka Topics<br>users-*.json] --> B[Nodes Import Module]
    end

    subgraph "Matching Pipeline"
        B --> C[Potential Matches Creation]
        C --> D[Perfect Matches Creation]
        D --> E[Match Transfer to Client]
    end

    subgraph "Storage"
        F[(PostgreSQL)]
        G[(MapDB - Staging)]
    end

    subgraph "Delivery"
        H[Client File System]
        I[Kafka: match-suggestions-*]
    end

    B --> F
    C --> G
    C --> F
    D --> F
    E --> H
    E --> I

    style A fill:#e3f2fd,stroke:#1565c0
    style B fill:#fff3e0,stroke:#ef6c00
    style C fill:#e8f5e9,stroke:#2e7d32
    style D fill:#f3e5f5,stroke:#6a1b9a
    style E fill:#ffebee,stroke:#c62828
    style F fill:#f5f5f5,stroke:#424242
    style G fill:#fff8e1,stroke:#ff8f00
```

---

## **2. Module Interaction Sequence**

```mermaid
sequenceDiagram
    participant Client
    participant Kafka
    participant Import as Nodes Import
    participant Potential as Potential Matches
    participant Perfect as Perfect Matches
    participant Transfer as Match Transfer
    participant DB as PostgreSQL
    participant FS as File System

    Client->>Kafka: Upload CSV / Ref IDs
    Kafka->>Import: Consume Message
    Import->>DB: Batch Upsert Nodes
    Note over Import,Potential: @Scheduled (Hourly)
    Import->>Potential: Trigger Batch Job
    Potential->>DB: Stream Nodes
    Potential->>Potential: LSH + Scoring
    Potential-->>DB: Save Candidates
    Note over Potential,Perfect: @Scheduled (3 AM IST)
    Potential->>Perfect: Trigger Optimization
    Perfect->>DB: Stream Candidates
    Perfect->>Perfect: Top-K / Hungarian / Auction
    Perfect-->>DB: Save Perfect Matches
    Perfect->>Transfer: Trigger Export
    Transfer->>DB: Stream Both Match Types
    Transfer->>FS: Export CSV/JSON
    Transfer->>Kafka: Publish MatchSuggestionsExchange
    Kafka-->>Client: Consume Suggestions
```

---

## **3. Class Diagram – Core Entities & Relationships**

```mermaid
classDiagram
    direction TB

    class Node {
        <<entity>>
        +UUID id
        +String referenceId
        +String groupId
        +UUID domainId
        +String type
        +Map~String,String~ metadata
        +LocalDateTime createdAt
    }

    class PotentialMatchEntity {
        <<entity>>
        +UUID id
        +String referenceId
        +String matchedReferenceId
        +Double compatibilityScore
        +UUID groupId
        +UUID domainId
        +String cycleId
        +LocalDateTime matchedAt
    }

    class PerfectMatchEntity {
        <<entity>>
        +UUID id
        +String referenceId
        +String matchedReferenceId
        +Double compatibilityScore
        +UUID groupId
        +UUID domainId
        +String cycleId
        +LocalDateTime matchedAt
    }

    class JobStatus {
        <<entity>>
        +UUID id
        +String groupId
        +UUID domainId
        +String status
        +Integer totalNodes
        +Integer successCount
        +Integer failedCount
        +LocalDateTime createdAt
        +LocalDateTime updatedAt
    }

    class MatchingConfiguration {
        <<entity>>
        +UUID groupId
        +MatchingAlgorithm algorithm
        +Integer nodeCountMin
        +Integer priority
        +Boolean isCostBased
        +Boolean isSymmetric
    }

    class MatchTransfer {
        <<dto>>
        +String senderId
        +String receiverId
        +Double score
        +String type
        +UUID groupId
        +UUID domainId
    }

    Node "1" --> "0..*" PotentialMatchEntity : generates
    Node "1" --> "0..*" PerfectMatchEntity : generates
    PotentialMatchEntity "1..*" --> "0..1" PerfectMatchEntity : optimized into
    JobStatus "1" --> "1" Node : tracks import
    MatchingConfiguration --> Node : applies to

    PotentialMatchEntity --> MatchTransfer : maps to
    PerfectMatchEntity --> MatchTransfer : maps to
```

---

## **4. Nodes Import Module – Detailed Class Diagram**

```mermaid
classDiagram
    class ScheduleXConsumer {
        +consume(record)
        +handleDLQ()
    }

    class ScheduleXPayloadProcessor {
        +processImportedNodesPayload(payload)
        +validateAndParse()
    }

    class ImportJobService {
        +startNodesImport(payload)
        +initiateNodesImport()
    }

    class NodesImportService {
        +processNodesImport(jobId, file|refIds)
        +processBatchesFromStream()
    }

    class NodesStorageProcessor {
        +processBatchAsync(batch)
        +executeCopy(csvData)
        +buildCsvRows()
    }

    class NodesImportStatusUpdater {
        +initiateNodesImport()
        +updateJobStatus()
        +completeJob()
    }

    class RetryTemplate {
        +execute(retryCallback)
    }

    ScheduleXConsumer --> ScheduleXPayloadProcessor
    ScheduleXPayloadProcessor --> ImportJobService
    ImportJobService --> NodesImportService
    NodesImportService --> NodesStorageProcessor
    NodesImportService --> NodesImportStatusUpdater
    NodesStorageProcessor --> RetryTemplate
    NodesImportStatusUpdater --> JobStatus
```

---

## **5. Potential Matches – Class Diagram with LSH & Graph Builders**

```mermaid
classDiagram
    class PotentialMatchesCreationScheduler {
        +processAllDomainsScheduled()
    }

    class PotentialMatchesCreationJobExecutor {
        +processGroup(groupId, domainId, cycleId)
    }

    class PotentialMatchService {
        +matchByGroup()
        +processGraphAndMatchesAsync()
    }

    class GraphPreProcessor {
        +build(nodes, request)
    }

    class SymmetricGraphBuilder {
        +build()
        +processChunk()
    }

    class BipartiteGraphBuilder {
        +build(left, right)
        +processBipartiteChunk()
    }

    class LSHIndex {
        +prepareAsync(nodes)
        +queryAsyncAll(chunk)
    }

    class MetadataCompatibilityCalculator {
        +calculate(pair)
    }

    class EdgeProcessor {
        +processBatchSync()
    }

    class QueueManager {
        +enqueue(matches)
        +flushQueueBlocking()
    }

    class GraphStore {
        +persistEdgesAsync()
        +streamEdges()
    }

    class PotentialMatchStorageProcessor {
        +savePotentialMatches()
    }

    PotentialMatchesCreationScheduler --> PotentialMatchesCreationJobExecutor
    PotentialMatchesCreationJobExecutor --> PotentialMatchService
    PotentialMatchService --> GraphPreProcessor
    GraphPreProcessor --> SymmetricGraphBuilder
    GraphPreProcessor --> BipartiteGraphBuilder
    SymmetricGraphBuilder --> LSHIndex
    SymmetricGraphBuilder --> EdgeProcessor
    EdgeProcessor --> MetadataCompatibilityCalculator
    SymmetricGraphBuilder --> QueueManager
    QueueManager --> GraphStore
    GraphStore --> PotentialMatchStorageProcessor
```

---

## **6. Perfect Matches – Strategy Pattern Class Diagram**

```mermaid
classDiagram
    class MatchingStrategy {
        <<interface>>
        +match(potentials, groupId, domainId) Map
    }

    class TopKWeightedGreedyStrategy {
        +match()
    }

    class AuctionApproximateStrategy {
        +match()
    }

    class HungarianStrategy {
        +match()
    }

    class HopcroftKarpStrategy {
        +match()
    }

    class MatchingStrategySelector {
        +select(context, groupId) MatchingStrategy
    }

    class PerfectMatchService {
        +processAndSaveMatches()
    }

    class PotentialMatchStreamingService {
        +streamAllMatches(consumer, batchSize)
    }

    class PerfectMatchStorageProcessor {
        +saveBatch()
    }

    MatchingStrategy <|.. TopKWeightedGreedyStrategy
    MatchingStrategy <|.. AuctionApproximateStrategy
    MatchingStrategy <|.. HungarianStrategy
    MatchingStrategy <|.. HopcroftKarpStrategy

    MatchingStrategySelector --> MatchingStrategy
    PerfectMatchService --> MatchingStrategySelector
    PerfectMatchService --> PotentialMatchStreamingService
    PerfectMatchService --> PerfectMatchStorageProcessor
```

---

## **7. Match Transfer – Producer-Consumer Class Diagram**

```mermaid
classDiagram
    class MatchesTransferScheduler {
        +scheduledMatchesTransferJob()
    }

    class MatchTransferService {
        +processGroup(groupId, domain)
    }

    class MatchTransferProcessor {
        +processMatchTransfer()
        +startProducers()
        +startConsumer()
    }

    class PotentialMatchStreamingService {
        +streamAllMatches(batchConsumer)
    }

    class PerfectMatchStreamingService {
        +streamAllMatches(batchConsumer)
    }

    class ExportService {
        +exportMatches(supplier)
    }

    class ScheduleXProducer {
        +sendMessage(topic, payload)
    }

    class BlockingQueue~MatchTransfer~ {
        +put()
        +poll(timeout)
    }

    MatchesTransferScheduler --> MatchTransferService
    MatchTransferService --> MatchTransferProcessor
    MatchTransferProcessor --> PotentialMatchStreamingService
    MatchTransferProcessor --> PerfectMatchStreamingService
    MatchTransferProcessor --> BlockingQueue
    MatchTransferProcessor --> ExportService
    ExportService --> ScheduleXProducer
```

---

## **8. Sequence Diagram – Cost-Based Node Import (Streaming CSV)**

```mermaid
sequenceDiagram
    participant Kafka
    participant Consumer
    participant Processor
    participant Orchestrator
    participant BatchEngine
    participant Storage
    participant DB

    Kafka->>Consumer: Record (GZIP CSV)
    Consumer->>Processor: processPayload()
    Processor->>Orchestrator: startNodesImport(file)
    Orchestrator->>Orchestrator: initiateJob(PENDING)
    Orchestrator->>BatchEngine: processNodesImport(jobId, file)
    BatchEngine->>BatchEngine: streamFromGZIP()
    loop Every 500 rows
        BatchEngine->>Storage: processBatchAsync(batch)
        Storage->>Storage: buildCSV()
        Storage->>DB: COPY temp_table
        DB-->>Storage: success
        Storage-->>BatchEngine: nodeIds
        BatchEngine->>Orchestrator: updateProgress()
    end
    BatchEngine->>Orchestrator: finalizeJob()
    Orchestrator->>DB: UPDATE status=COMPLETED
```

---

## **9. Sequence Diagram – LSH-Based Potential Matching (Symmetric)**

```mermaid
sequenceDiagram
    participant Scheduler
    participant JobExecutor
    participant GraphBuilder
    participant LSH
    participant EdgeProc
    participant Queue
    participant Saver

    Scheduler->>JobExecutor: processGroup()
    JobExecutor->>GraphBuilder: build(nodes)
    GraphBuilder->>LSH: prepareAsync(nodes)
    LSH-->>GraphBuilder: indexed
    loop For each chunk
        GraphBuilder->>EdgeProc: processBatch(chunk)
        EdgeProc->>LSH: queryAsyncAll(node)
        LSH-->>EdgeProc: candidates
        EdgeProc->>EdgeProc: scorePairs()
        EdgeProc-->>GraphBuilder: matches
        GraphBuilder->>Queue: enqueue(matches)
    end
    GraphBuilder->>Saver: finalize()
    Saver->>Saver: stream from MapDB
    Saver->>Saver: group by refId → top-K
    Saver->>DB: UPSERT final matches
```

---

## **10. Sequence Diagram – Match Transfer (Producer-Consumer)**

```mermaid
sequenceDiagram
    participant Scheduler
    participant Processor
    participant PotStream
    participant PerfStream
    participant Queue
    participant Export
    participant KafkaProd

    Scheduler->>Processor: processMatchTransfer()
    Processor->>PotStream: streamAllMatches(consumer)
    Processor->>PerfStream: streamAllMatches(consumer)
    PotStream->>Queue: put(potentialBatch)
    PerfStream->>Queue: put(perfectBatch)
    Processor->>Export: exportMatches(supplier)
    Export->>Queue: poll(300ms)
    Queue-->>Export: batch
    Export->>Export: writeToFile()
    Export->>KafkaProd: sendMessage(topic, payload)
    Note over PotStream,PerfStream: Both complete → set done=true
    Export->>Queue: poll() → null → exit
```

---

## **11. State Diagram – Job Lifecycle**

```mermaid
stateDiagram-v2
    [*] --> PENDING
    PENDING --> PROCESSING: startImport()
    PROCESSING --> PROCESSING: batchProcessed()
    PROCESSING --> COMPLETED: allBatchesDone()
    PROCESSING --> FAILED: error / timeout
    COMPLETED --> [*]
    FAILED --> [*]
    note right of FAILED: Retryable via DLQ
```

---

## **12. Deployment & Runtime Topology**

```mermaid
graph TB
    subgraph "Kubernetes Pod"
        S1[Scheduler<br>@Scheduled]
        W1[Worker 1]
        W2[Worker 2]
    end

    subgraph "Thread Pools"
        TP1[nodesImportExecutor<br>core=4, max=8]
        TP2[matchTransferExecutor<br>core=4, max=8]
        TP3[potentialMatchExecutor<br>core=8]
    end

    subgraph "External"
        K[(Kafka)]
        P[(PostgreSQL)]
        M[(MapDB)]
        F[File Share]
    end

    S1 --> TP1
    W1 --> TP2
    W2 --> TP3
    TP1 --> P
    TP3 --> M
    TP3 --> P
    TP2 --> F
    TP2 --> K
```

---

## **13. Data Flow Diagram (DFD) – Level 1**

```mermaid
flowchart LR
    D1[Kafka Message] --> P1[Validate & Parse]
    P1 --> D2{File or Ref IDs?}
    D2 -->|File| P2[Stream GZIP CSV]
    D2 -->|Ref IDs| P3[Convert to Nodes]
    P2 --> P4[Batch Partition]
    P3 --> P4
    P4 --> P5[Upsert via COPY]
    P5 --> DB[(PostgreSQL)]
    P5 --> M1[Update Job Status]

    subgraph Potential
        DB --> S1[Stream Nodes]
        S1 --> S2[LSH Index]
        S2 --> S3[Candidate Gen]
        S3 --> S4[Score & Buffer]
        S4 --> MapDB[(MapDB)]
    end

    subgraph Perfect
        DB --> T1[Stream Potentials]
        T1 --> T2[Strategy Select]
        T2 --> T3[Top-K / Auction]
        T3 --> T4[Bulk Save]
        T4 --> DB
    end

    subgraph Transfer
        DB --> U1[Dual Stream]
        U1 --> U2[Queue Buffer]
        U2 --> U3[Export + Kafka]
        U3 --> FS[File]
        U3 --> K[Kafka]
    end
```

---

## **Why This System Excels**

| Feature | Implementation |
|-------|----------------|
| **Zero OOM** | Memory-aware batching, GC hooks |
| **No Data Loss** | Idempotent upserts, DLQ |
| **Full Traceability** | `jobId`, `cycleId` in logs & DB |
| **Configurable Algorithms** | Strategy pattern + DB config |
| **Production Ready** | Circuit breakers, retries, fallbacks |

---

**GraphMatch Engine** — *Precision at Scale, Built to Last.*