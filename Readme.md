ScheduleX — Overview (with Design Doc Links)
===============================================================

What this system does
---------------------
This platform takes raw entity data (called “nodes”), computes which nodes are compatible with each other, selects the best matches, and then delivers those results to client-facing systems.

It runs as a pipeline made of four major stages:

1) Nodes Import
2) Potential Matches Creation
3) Perfect Match Creation
4) Match Transfer to Client

Each stage is designed to handle large volumes, run safely on schedules or triggers, and produce outputs that are reliable to re-run.

High-level flow in one sentence
-------------------------------
We ingest nodes into PostgreSQL → compute candidate relationships (potential matches) → refine those candidates into final selections (perfect matches) → export results to a file and notify downstream consumers.

```mermaid
flowchart LR
  EXT[External Systems] -->|Import request| KAFKA[(Kafka)]
  EXT -->|CSV.GZ upload| S3[(MinIO/S3)]

  KAFKA --> NIS[Nodes Import]
  S3 --> NIS
  NIS --> PG[(PostgreSQL)]

  PG --> PMS[Potential Matches Creation]
  PMS --> LMDB[(LMDB Edge Store)]

  LMDB --> PMCS[Perfect Match Creation]
  PMCS --> PG

  PG --> MTS[Match Transfer]
  MTS --> FILE[(Export File)]
  MTS --> TOPIC[(Match Suggestions Topic)]
  TOPIC --> CLIENTS[Downstream / Client Systems]

```

Key terms 
--------------------------
- Node: A single entity we want to match (user/product/resource).
- Potential Match: A candidate pairing between two nodes, usually many per node, with a compatibility score.
- Perfect Match: The best match(es) selected from the candidates based on configured rules.
- Domain / Group: Logical partitions for multi-tenancy and business segmentation (matching runs per domain+group).
- Cursor / Run State: A saved “position” so incremental processing can resume and avoid reprocessing everything.
- LMDB: A fast local storage used to read/write large edge sets efficiently (often treated as regenerable/staging).
- Export Artifact: A file containing match outputs in a client-consumable format.
- Notification Event: A message (Kafka-like) that tells downstream systems “your file is ready” and where to fetch it.

Stage 1 — Nodes Import 
----------------------------------------
Goal: Bring large sets of nodes into PostgreSQL quickly and safely.

In practice:
- An upstream system requests an import by sending a message.
- The request either points to a compressed CSV file in object storage (MinIO/S3) or includes a reference list.
- The import service streams and parses the input in batches to avoid memory spikes.
- Nodes are written efficiently using PostgreSQL bulk operations (COPY + merge/UPSERT).
- The system records job status so operators and downstream systems know whether the import succeeded, partially succeeded, or failed.

Output:
- Nodes (and metadata) stored in PostgreSQL.
- A job status update (persisted and typically published).

Why this stage matters:
- All later stages depend on clean, consistent node data.
- Idempotent writes let the system safely retry without creating duplicates.

### i) File-Based Import Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant Consumer
    participant JobSvc as Import Job Service
    participant FileSvc as File Import Service
    participant MinIO
    participant Parser as CSV Parser
    participant Processor as Import Processor
    participant Storage as Storage Processor
    participant DB as PostgreSQL
    participant Status as Status Updater
    
    Kafka->>Consumer: NodeExchange message (filePath)
    Consumer->>JobSvc: startNodesImport(payload)
    
    JobSvc->>JobSvc: Validate payload (file-based)
    JobSvc->>DB: Insert NodesImportJob (PENDING)
    DB-->>JobSvc: jobId
    
    JobSvc->>JobSvc: Resolve MultipartFile
    
    alt Remote file (http/https)
        JobSvc->>MinIO: Download file via S3 API
        MinIO-->>JobSvc: RemoteMultipartFile
    else Local file
        JobSvc->>JobSvc: FileSystemMultipartFile (local path)
    end
    
    JobSvc->>FileSvc: processNodesImport(jobId, file, exchange)
    FileSvc->>DB: Update job status (PROCESSING)
    
    FileSvc->>FileSvc: Open GZIP stream
    FileSvc->>Parser: parseInBatches(gzipStream, callback)
    
    loop For each 1000 rows
        Parser-->>FileSvc: Batch of NodeResponse
        FileSvc->>FileSvc: processBatchAsync (thread pool)
        
        par Parallel Batch Processing
            FileSvc->>Processor: processBatch(jobId, batch, ...)
            Processor->>Processor: Convert NodeResponse → Node
            Processor->>Storage: saveNodesSafely(nodes)
            
            Storage->>Storage: Generate UUIDs if missing
            Storage->>DB: COPY to temp_nodes (binary)
            Storage->>DB: INSERT ON CONFLICT (upsert)
            DB-->>Storage: Returning nodeId, referenceId
            
            Storage->>DB: Batch insert metadata
            
            Processor->>DB: Increment processed count
            Processor->>Processor: Track success/failed lists
        end
        
        FileSvc->>FileSvc: orTimeout(calculateBatchTimeout)
    end
    
    FileSvc->>FileSvc: joinAndClearFutures (wait all)
    FileSvc->>Status: finalizeJob(success, failed, total)
    
    alt All succeeded
        Status->>DB: Update job (COMPLETED)
        Status->>Kafka: Publish success status
    else Some failed
        Status->>DB: Update job (FAILED)
        Status->>Kafka: Publish failure status
    end
```

### ii) Reference-Based Import Sequence

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
    JobSvc->>JobSvc: Partition into 1000-node batches
    
    JobSvc->>BatchSvc: processNodesImport(jobId, referenceIds, groupId, ...)
    BatchSvc->>DB: Update job status (PROCESSING)
    BatchSvc->>DB: Update total nodes count
    
    loop For each batch
        BatchSvc->>BatchSvc: processBatchAsync (thread pool)
        
        par Parallel Processing
            BatchSvc->>Processor: processAndPersist(jobId, groupId, nodes, ...)
            
            Processor->>DB: saveAll(batch) via JPA
            DB-->>Processor: Saved entities
            
            Processor->>Processor: Track success list
            Processor->>DB: Increment processed count
        end
        
        BatchSvc->>BatchSvc: orTimeout(calculateBatchTimeout)
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

Stage 2 — Potential Matches Creation (creating candidates)
----------------------------------------------------------
Goal: Generate “candidate match edges” between nodes in each domain/group.

In practice:
- Runs on a schedule and/or incrementally.
- Reads nodes from PostgreSQL and uses a matching strategy based on data shape and configuration:
    - LSH (Locality-Sensitive Hashing) for large-scale similarity detection
    - Metadata-weighted comparisons when metadata keys and weights matter
    - Flat strategy as a simple fallback for small sets or limited metadata
- Stores candidate edges in LMDB (and optionally PostgreSQL).

Output:
- Candidate edges (potential matches) stored where the next stage can read them efficiently (commonly LMDB).

Why this stage matters:
- It reduces an otherwise explosive “compare everything with everything” problem into a smaller candidate set.

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler
    participant EXE as JobExecutor
    participant FETCH as NodeFetchService
    participant PG as PostgreSQL
    participant STRAT as MatchingStrategy
    participant PROC as ComputationProcessor
    participant LMDB as LMDB Edge Store
    participant STORE as StorageProcessor

    SCH->>SCH: Scheduled trigger
    SCH->>EXE: processGroup(groupId, domainId)

    loop Until cursor exhausted
        EXE->>FETCH: fetchNodeBatch(groupId)
        FETCH->>PG: Select nodes by cursor
        PG-->>FETCH: Node batch
        FETCH-->>EXE: Nodes
    end

    EXE->>STRAT: determineStrategy(nodes)
    STRAT->>PROC: computePotentialMatches(nodes)

    par Parallel computation
        PROC->>PROC: Compare & score nodes
        PROC->>PROC: Generate candidate edges
    end

    PROC->>LMDB: persistEdgesAsync()
    PROC->>STORE: savePotentialMatches()

    SCH->>SCH: Release domain/group semaphore


```

Stage 3 — Perfect Match Creation (choosing final matches)
---------------------------------------------------------
Goal: Turn candidate edges into final “best match” results that the business can act on.

In practice:
- Runs on a schedule per domain/group.
- Streams edges from LMDB to avoid loading huge graphs in memory.
- Applies the configured algorithm:
    - Symmetric: treat the match relationship as mutual (requires canonicalization to prevent duplicates)
    - Asymmetric: treat matches as directional (one-way preference/selection rules)
    - pruning (often Top-K per node)
- Writes final results into PostgreSQL using bulk persistence and deduplication rules.

Output:
- Perfect matches stored in PostgreSQL (typically with cycle/run identifiers and timestamps).

Why this stage matters:
- It produces the final deliverable matching output used by client-facing features and reporting.

```mermaid
sequenceDiagram
    autonumber
    participant SCH as Scheduler
    participant SVC as CreationService
    participant EDGE as EdgePersistence
    participant LMDB as LMDB Store
    participant STRAT as MatchingStrategy
    participant STORE as StorageProcessor
    participant PG as PostgreSQL

    SCH->>SVC: processGroup(groupId, domainId)
    SVC->>PG: Load last run state

    EDGE->>LMDB: Open edge stream
    loop Stream edges
        LMDB-->>EDGE: Edge
        EDGE-->>SVC: Edge batch
    end

    SVC->>SVC: Build adjacency map
    SVC->>SVC: Prune to Top-K per node
    SVC->>STRAT: Select best matches
    STRAT-->>SVC: Final matches

    SVC->>STORE: savePerfectMatches()
    STORE->>PG: Bulk upsert results

    SVC->>PG: Update run status


```

Stage 4 — Match Transfer to Client (export + notify)
----------------------------------------------------
Goal: Provide clients/downstream systems an easy-to-consume artifact and a reliable notification.

In practice:
- Runs on a schedule per domain/group.
- Streams potential matches and perfect matches out of PostgreSQL using JDBC streaming.
- Converts database entities into a transfer-friendly DTO format.
- Merges both sources via a bounded queue (producer-consumer pipeline) to control memory and add backpressure.
- ExportService writes the data to a file (the client artifact).
- A notification event is published containing the file reference (path/URI, metadata such as counts/checksum).

Output:
- Exported match file (stored in agreed storage).
- A messaging event telling consumers where the file is.

Why this stage matters:
- It decouples internal storage schemas from client consumption needs.
- It supports high-volume delivery without requiring clients to query large DB tables directly.

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

How the pipeline fits together
------------------------------
- Nodes Import must succeed before meaningful matching can occur.
- Potential Matches builds candidate edges from nodes.
- Perfect Match selects the final best matches from those candidates.
- Match Transfer exports and announces results for downstream systems.

Operational model (how it runs safely)
--------------------------------------
- Batch/scheduled execution: predictable load and simpler operations.
- Concurrency controls: limits how many domains/groups run at once to protect DB and I/O.
- Backpressure: bounded queues prevent “fast producers” from overwhelming “slow consumers.”
- Resilience:
    - Retries for transient failures
    - Circuit breakers to prevent cascading failures
    - Clear per-group failure isolation where possible
- Observability:
    - metrics for throughput, duration, failures
    - executor health and queue depth monitoring
    - structured logs with correlation fields (domainId, groupId, jobId/cycleId)

Links to Design Documents
------------------------
Below are the documents this overview is based on. 

High-Level Designs (HLD)
- Nodes Import System — HLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/High%20Level%20Designs/NodesImport.md
- Potential Matches Creation System — HLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/High%20Level%20Designs/ScheduledPotentialMatchesCreation.md
- Perfect Match Creation System — HLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/High%20Level%20Designs/ScheduledPerfectMatchesCreation.md
- Match Transfer to Client — HLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/High%20Level%20Designs/MatchesTransfer.md

Low-Level Designs (LLD)
- Nodes Import System — LLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/Low%20Level%20Designs/NodesImport.md
- Potential Matches Creation System — LLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/Low%20Level%20Designs/ScheduledPotentialMatchesCreation.md
- Perfect Match Creation System — LLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/Low%20Level%20Designs/ScheduledPerfectMatchesCreation.md
- Match Transfer to Client — LLD  
  Link: https://github.com/kagit00/schedule_x/blob/master/docs/Low%20Level%20Designs/MatchesTransfer.md

Suggested reading order 
-------------------------------------------
1) End-to-end overview (this document)
2) Nodes Import HLD + LLD
3) Potential Matches HLD + LLD
4) Perfect Match HLD + LLD
5) Match Transfer HLD + LLD


Notes 
---------------------
- PostgreSQL is the system of record for nodes and match outputs.
- LMDB is used as a performance layer for edge streaming and may be treated as regenerable depending on implementation.
- Export files and notifications are the public “delivery contract” for clients/downstream systems.
