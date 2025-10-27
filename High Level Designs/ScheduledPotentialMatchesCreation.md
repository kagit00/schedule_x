# Scheduled Potential Match Creation — High‑Level Design (HLD)

## Table of Contents
- [1) Purpose and Scope](#1-purpose-and-scope)
- [2) Architecture Overview](#2-architecture-overview)
- [3) End-to-End Flow](#3-end-to-end-flow)
- [4) Components and Responsibilities](#4-components-and-responsibilities)
- [5) Concurrency and Backpressure](#5-concurrency-and-backpressure)
- [6) Data and Persistence](#6-data-and-persistence)
- [7) Resilience and Error Handling](#7-resilience-and-error-handling)
- [8) Observability](#8-observability)
- [9) Configuration & Tuning](#9-configuration--tuning)
- [10) Scaling & Capacity Planning](#10-scaling--capacity-planning)
- [11) Security & Data Integrity](#11-security--data-integrity)
- [12) Trade-offs & Known Considerations](#12-trade-offs--known-considerations)
- [13) Deployment & Operational Notes](#13-deployment--operational-notes)
- [14) Sequence Flows](#14-sequence-flows)
- [15) Glossary](#15-glossary)

---

## 1) Purpose and Scope
- Periodically compute and persist “potential matches” between nodes by domain/group using configurable graph strategies.
- Batch-oriented pipeline with strong backpressure and resilience.
- Outputs:
    - Transient edges to an embedded MapDB for staging/streaming.
    - Finalized potential_matches upserted into Postgres.

---

## 2) Architecture Overview
- Orchestration layer drives scheduled cycles across domains/groups with concurrency gating.
- Matching pipeline fetches nodes, selects a weight function, builds graph(s), and computes potential matches.
- Persistence pipeline buffers matches in bounded queues, drains to MapDB and then Postgres, with periodic/boosted/blocking flushes.
- Finalization streams edges from MapDB per group to produce final DB writes and cleanup.

High-level (logical) view:
```
Scheduler → JobExecutor → PotentialMatchService
    └─> NodeFetchService (IDs/Nodes)
    └─> WeightFunctionResolver
    └─> GraphPreProcessor
           ├─ SymmetricGraphBuilder
           │     └─ (Flat/Metadata+LSH Strategy) → PotentialMatchComputationProcessor
           └─ BipartiteGraphBuilder
                 └─ Chunked left×right → GraphStore → finalize
    └─> Node mark processed + participation history flush

PotentialMatchComputationProcessor
    └─ QueueManager → periodic/boosted/blocking flush →
         ├─ GraphStore (MapDB) staging
         └─ PotentialMatchSaver → PotentialMatchStorageProcessor (Postgres)
```

---

## 3) End-to-End Flow
- Scheduled cycle:
    - Enumerate active domains → groups, acquire semaphores (domain + per-group), run per-group job.
    - After all groups complete: finalize cycle and flush remaining queues.
- Per-group pipeline:
    - Page loop: fetch node IDs → fetch nodes → resolve weight function → build graph (symmetric/bipartite).
    - Builders produce PotentialMatch records (chunked); enqueue them for draining to MapDB and Postgres.
    - Mark nodes as processed; periodically flush participation history.

---

## 4) Components and Responsibilities
- PotentialMatchesCreationScheduler
    - Scheduled entrypoint; builds task list; semaphores for domain/group; triggers JobExecutor and a final flush loop; invokes finalizer.
- PotentialMatchesCreationJobExecutor
    - Paged per-group processing with retry/backoff; limits concurrent pages per group via pageSemaphore.
- PotentialMatchServiceImpl
    - Fetch node IDs/nodes; derive limit and weight function; drive GraphPreProcessor; mark nodes processed; buffer/flush participation history.
- NodeFetchService
    - Async, transactional fetch with timeouts; partitioned fetch of nodes; marks nodes processed.
- WeightFunctionResolver
    - Chooses a metadata-based weightFunctionKey per group; registers a configurable weight function if absent.
- GraphPreProcessor
    - Selects strategy: SYMMETRIC vs BIPARTITE vs AUTO+partition; global build concurrency (semaphore) and deadline watchdog.
- SymmetricGraphBuilder
    - Index nodes, chunk computation with backpressure (compute/mapping semaphores), retry on chunks; stream edges for final save; cleanup.
- SymmetricEdgeBuildingStrategyFactory
    - Produces Flat or Metadata+LSH strategies for symmetric builds.
- MetadataEdgeBuildingStrategy
    - Encodes metadata; queries LSH for candidates; computes matches per chunk and forwards to processor.
- BipartiteGraphBuilder
    - Cartesian chunking across left/right partitions; processes chunk matches; persists chunk results to MapDB; rebuilds graph from MapDB for finalization; cleans up.
- BipartiteEdgeBuilder
    - Computes compatibility scores for left×right node pairs with thresholding; emits matches and edges.
- PotentialMatchComputationProcessorImp
    - Per-group queue manager; enqueues/de-duplicates; dynamic flush; draining to MapDB and Postgres; blocking flush for shutdown; finalization path.
- QueueManagerFactory/QueueManagerImpl
    - Bounded per-group queues; periodic/boosted/blocking flush orchestration; TTL eviction.
- GraphStore (MapDB)
    - Transient edges (keyed by groupId:chunkIndex:refId:matchedRefId); batched writes with commit; stream by group; cleanup.
- PotentialMatchSaver/PotentialMatchStorageProcessor
    - Async Postgres persistence via temp table + COPY + upsert; deletion and counting; concurrency limited.

---

## 5) Concurrency and Backpressure
- Semaphores:
    - domainSemaphore (max domains), per-group groupLocks, pageSemaphore (pages per group).
    - Graph build: buildSemaphore.
    - Symmetric builder: computeSemaphore (chunk compute), mappingSemaphore (mapping/persist).
    - QueueManager: periodicFlush, boostedFlush, blockingFlush semaphores.
    - Storage: saveSemaphore (final save), storageSemaphore (DB concurrency).
- Executors (separation of concerns):
    - Batch/orchestration: matchCreationExecutorService
    - IO: ioExecutorService
    - Graph: graphExecutorService / graphBuildExecutor
    - Persistence: persistenceExecutor / matchesProcessExecutor / matchesStorageExecutor
    - Queue flush: queueFlushExecutor + queueFlushScheduler
    - Watchdog: watchdogExecutor
- Backpressure:
    - Bounded queues (capacity); dynamic flush intervals; boosted drains at thresholds; drops with metrics on timeouts/full queues.
    - Page/batch/chunk limits cap computation and memory.

---

## 6) Data and Persistence
- Core records:
    - Node: id, referenceId, type, createdAt, metaData
    - GraphRecords.PotentialMatch: referenceId, matchedReferenceId, compatibilityScore, groupId, domainId
    - PotentialMatchEntity: DB upsert record with processing_cycle_id and timestamps
- MapDB (GraphStore):
    - Key: groupId:chunkIndex:referenceId:matchedReferenceId → serialized PotentialMatch
    - Batched persist with commit; streaming and cleanup by group
- Postgres (PotentialMatchStorageProcessor):
    - COPY to temp table + upsert into public.potential_matches
    - Upsert key: (group_id, reference_id, matched_reference_id)
    - Incremental (chunk) and finalization save paths

---

## 7) Resilience and Error Handling
- Timeouts:
    - Semaphore acquisitions, async tasks, chunk processing, flushes, MapDB commits, DB saves
- Retries & backoff:
    - Page processing (configurable)
    - Symmetric chunk processing (retry with exponential backoff)
    - Storage layer (@Retryable for batch saves/deletes)
- Circuit breakers / fallbacks:
    - BipartiteGraphBuilder (Resilience4j @Retry/@CircuitBreaker with fallback)
    - PotentialMatchComputationProcessor fallbacks for chunk and pending save
    - GraphStore fallbacks for persist/stream/clean
- Shutdown:
    - Flush queues (blocking), remove instances, cleanup GraphStore; orderly executor termination

---

## 8) Observability
- Metrics (Micrometer):
    - End-to-end: batch_matches_total_duration, matching_duration, nodes_processed_total, matches_generated_total
    - Graph builds: graph_preprocessor_duration, graph_build_duration, chunk timers, errors/timeouts
    - Queues: queue size/fill ratio, flush rates, queue_drain_warnings_total, match_drops_total
    - Storage: storage_processor_* duration/counters; upsert totals; delete/count metrics
    - MapDB: persist latency, commit latency, stream/clean latency, error counters, map size; executor gauges
    - Executors: queue length and active count per pool
- Logging:
    - Rich tagging with groupId, domainId, cycleId, page/chunk; clear warnings for backpressure, timeouts, partial pages

---

## 9) Configuration & Tuning
Selected keys (representative; see code for full list):
- Scheduling & concurrency
    - match.save.delay, match.max-concurrent-domains
- Paging & limits
    - match.batch-limit, nodes.limit.full.job, node-fetch.batch-size, MAX_NODES per page
- Retries/timeouts
    - match.max-retries, match.retry-delay-millis, node-fetch.future-timeout-seconds, graph.chunk-processing-timeout-seconds
- Graph build
    - graph.max-concurrent-builds, graph.chunk-size, graph.max-concurrent-batches, graph.max-concurrent-mappings, graph.top-k
- Queue & flush
    - match.queue.capacity, match.flush.interval-seconds, match.flush.min-interval-seconds, match.flush.queue-threshold, match.queue.drain-warning-threshold, match.boost-batch-factor, match.max-final-batch-size
- Storage
    - matches.save.batch-size, match.save.timeout-seconds, match.final-save.timeout-seconds
- MapDB
    - mapdb.path, mapdb.batch-size, mapdb.commit-threads
- Bipartite
    - bipartite.edge.build.similarity-threshold

Tuning guidance:
- Increase maxConcurrentDomains, pool sizes, and queue capacity to scale throughput.
- Adjust chunkSize, node-fetch batch-size, and commit threads to match I/O bandwidth.
- Use flush thresholds and boosted drains to manage bursty loads.

---

## 10) Scaling & Capacity Planning
- Horizontal scaling via domain/group concurrency and executor pools.
- Backpressure ensures bounded memory; queues provide smoothing for bursty producers.
- MapDB commit latency and queue flush throughput are critical—monitor and tune batch sizes and commit threads.
- Consider SSD-backed local paths for MapDB for low-latency persistence.

---

## 11) Security & Data Integrity
- Upsert semantics prevent duplicates in Postgres on (group_id, reference_id, matched_reference_id).
- MapDB content is transient and per-group cleaned after finalization.
- Ensure secure and writable mapdb.path; DB credentials managed via Hikari configuration (externalized).
- Idempotent finalization: re-reading edges for final write is safe due to upsert behavior.

---

## 12) Trade-offs & Known Considerations
- Two-stage persistence (MapDB → Postgres) adds I/O but provides backpressure and operational decoupling.
- Some blocking waits inside async flows require adequate executor sizing to prevent starvation.
- MetadataEncoder batch-global cache reset is not ideal if shared—prefer per-batch or strategy-local instances to avoid contention.
- Potential double-processing path risk (in symmetric pipeline) if chunk results are both handled immediately and later re-mapped—verify dedupe/desired behavior.
- Naming consistency (e.g., Impl/Imp) and minor log/metric duplication in GraphStore can be polished.

---

## 13) Deployment & Operational Notes
- Prerequisites:
    - Postgres reachable with sufficient write throughput.
    - Local disk path for MapDB (mapdb.path) with write permissions and adequate space.
- Shutdown:
    - Module flushes queues, removes queue managers, cleans GraphStore per group, and closes executors.
- Portability:
    - Ensure mapdb.path configured appropriately for the target OS/container (default Windows-like path should be overridden).

---

## 14) Sequence Flows

### Symmetric Strategy Flow (per group)
```
Scheduler
  → JobExecutor.processGroup (page loop)
    → PotentialMatchService.matchByGroup
      → NodeFetchService.fetchNodeIdsAsync → fetchNodesInBatchesAsync
      → WeightFunctionResolver.resolveWeightFunctionKey
      → GraphPreProcessor.build (SYMMETRIC)
         → SymmetricGraphBuilder.indexNodes
         → processNodeChunks:
             for each chunk:
               Strategy.processBatch → PotentialMatchComputationProcessor.processChunkMatches
                 → QueueManager.enqueue → periodic/boosted flush
                   → GraphStore.persistEdgesAsync + PotentialMatchSaver.savePotentialMatches
         → savePendingMatches
         → finalize: streamEdges(MapDB) → saveFinalMatches(Postgres) → getFinalMatchCount → cleanup
      → NodeFetchService.markNodesAsProcessed
      → Periodic participation history save
```

### Bipartite Strategy Flow (per group)
```
GraphPreProcessor.build (BIPARTITE)
  → BipartiteGraphBuilder.processChunks:
      - Partition left/right into chunks
      - submitChunk for each left×right pair:
         → processBipartiteChunk → PotentialMatch list
         → GraphStore.persistEdgesAsync (per chunk)
  → Wait for all chunk persists
  → Stream edges from MapDB:
      - Rebuild graph
      - Convert edges to matches
      - Finalize (save to Postgres)
  → Cleanup GraphStore
```

---

## 15) Glossary
- Domain/Group: Logical segmentation of nodes for matching.
- CycleId: Unique identifier for a scheduled processing cycle for correlation.
- MatchType: SYMMETRIC (same-type nodes) vs BIPARTITE (left/right partitions), or AUTO inference.
- LSH: Locality Sensitive Hashing, used to narrow candidate pairs for metadata-based matching.
- MapDB: Embedded key/value store used as a transient graph edge staging area.

--- 

End of HLD.