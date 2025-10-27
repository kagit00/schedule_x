# Scheduled Perfect Matches Creation — High‑Level Design (HLD)

## 1) Purpose and Scope
- Objective: On a daily schedule, compute and persist “perfect matches” for each group/domain by consuming existing potential matches and applying a configured algorithm.
- Scope:
    - Batch/scheduled execution only (no real-time).
    - Reads PotentialMatchEntity from DB via streaming.
    - Applies a selected MatchingStrategy (from DB-driven configuration).
    - Writes PerfectMatchEntity to Postgres.
    - Tracks per-group last run status to avoid unnecessary reprocessing.

---

## 2) Triggers and Entry Points
- Scheduler: PerfectMatchesCreationScheduler.createPerfectMatches
    - Cron: 0 0 3 * * * (Asia/Kolkata)
    - Discovers eligible tasks via PerfectMatchCreationService.getTasksToProcess:
        - Reprocess if processedNodes > lastRun.nodeCount OR lastRun.status ∈ {PENDING, FAILED}.

---

## 3) Architecture Overview
- Orchestration
    - PerfectMatchesCreationScheduler
        - Time-based trigger; per-group execution guarded by Retry/CircuitBreaker.
        - Clears MatchCache for the group before processing.
    - PerfectMatchCreationService
        - Builds task list across active domains and groups; coordinates domain/group concurrency.
        - Manages LastRunPerfectMatches state (PENDING/COMPLETED/FAILED, nodeCount, runDate).
    - PerfectMatchCreationJobExecutor
        - Executes a group with retries/backoff and per-group concurrency cap.

- Matching Pipeline
    - PerfectMatchServiceImpl
        - Builds MatchingContext from DB config and inferred MatchType.
        - Streams potential matches in pages; memory-aware sub-batching and CPU-parallel processing.
        - Forms per-node top‑K adjacency; delegates to MatchingStrategy; persists final matches.

- Strategy Layer (pluggable, selected via MatchingConfiguration)
    - AuctionApproximateMatchingStrategy (auction/bidding, approximate one-to-one).
    - HopcroftKarpMatchingStrategy (maximum bipartite matching).
    - HungarianMatchingStrategy (optimal assignment).
    - TopKWeightedGreedyMatchingStrategy (top‑K per node, memory-aware streaming).

- Data Access and Persistence
    - PotentialMatchStreamingService: forward-only JDBC streaming of PotentialMatchEntity with retries.
    - PerfectMatchSaver → PerfectMatchStorageProcessor: async batched DB writes (COPY to temp table + upsert).

- Utilities
    - GraphPreProcessor.determineMatchType: infers SYMMETRIC/BIPARTITE from sample matches and node types.
    - MatchesCreationFinalizer: signaled after batch completion.

---

## 4) End‑to‑End Flow (Per Cycle)
1) Scheduler builds (domain, group) tasks list.
2) For each task:
    - Mark lastRun PENDING; clear MatchCache for group.
    - Acquire domain and group semaphores; execute group via PerfectMatchCreationJobExecutor.
    - JobExecutor calls PerfectMatchService.processAndSaveMatches with retries.
    - On success: mark lastRun COMPLETED with current processed node count.
    - On failure: mark lastRun FAILED.
    - Release semaphores.
3) After all tasks: call MatchesCreationFinalizer.finalize(true).

---

## 5) Components and Responsibilities

- PerfectMatchesCreationScheduler
    - Cron trigger; iterates tasks; wraps per-group run in @Retry/@CircuitBreaker with fallback updating lastRun to FAILED.
    - Metrics: “perfect_matches_creation” timer, error counters.

- PerfectMatchCreationService
    - Discovers tasks from DomainService + MatchingGroupRepository, filters by LastRunPerfectMatches and processed node count.
    - Concurrency: fair domainSemaphore (max concurrent domains), fair groupSemaphore (max concurrent groups).
    - Batch coordination and timing; persists lastRun updates.

- PerfectMatchCreationJobExecutor
    - Per-group execution with maxRetries and exponential backoff (retryDelayMillis).
    - Per-group semaphore to throttle parallel groups; prunes its map when idle.

- PerfectMatchServiceImpl
    - Builds MatchingContext (DB configuration + inferred MatchType).
    - Streams PotentialMatchEntity using PotentialMatchStreamingService with configurable fetch size.
    - Memory/backpressure-aware processing:
        - cpuTaskSemaphore (~2×CPU) gates CPU work per streamed batch.
        - Dynamic sub-batch sizing based on live memory; halts all processing if memory threshold exceeded.
        - Builds per-node top‑K candidates via bounded PriorityQueues.
    - Invokes MatchingStrategy to compute perfect matches.
    - Persists PerfectMatchEntity in batches via PerfectMatchSaver with timeouts.
    - Metrics: matching_duration, processed counts, saved totals, errors.

- MatchingStrategySelector
    - Resolves strategy bean by algorithm id in MatchingConfiguration for the group.

- PotentialMatchStreamingService
    - JDBC streaming (forward-only, read-only, fetchSize batches) with retry and exponential backoff.
    - Also exposes a JPA stream for sampling (match type inference).

- PerfectMatchSaver
    - Facade for async saving with a top-level timeout; guards shutdown state.

- PerfectMatchStorageProcessor
    - Asynchronous batched DB writes on ioExecutor.
    - For each batch:
        - COPY to temp_perfect_matches (binary).
        - Upsert into public perfect matches table via QueryUtils.getUpsertPerfectMatchesSql().
        - Transactional, @Retryable on SQL/Timeout with exponential backoff.
    - Metrics: per-batch duration and totals; error counters.
    - Shutdown: orderly ioExecutor termination and datasource close.

---

## 6) Data Flow and Persistence
- Inputs:
    - PotentialMatchEntity stream (groupId, domainId, referenceId, matchedReferenceId, score).
    - MatchingConfiguration/MatchingGroup/MatchingAlgorithm from DB.
- Transformations:
    - Build per-node top‑K adjacency.
    - Apply chosen matching strategy to generate final results (MatchResult).
- Outputs:
    - PerfectMatchEntity (groupId, domainId, referenceId, matchedReferenceId, compatibilityScore, matchedAt).
    - LastRunPerfectMatches updated for observability and re-run control.

Persistence details:
- PerfectMatchStorageProcessor.savePerfectMatches
    - Partition input by import.batch-size (default 1000).
    - For each batch: COPY to temp_perfect_matches → upsert (ON CONFLICT) into final table.
    - Timeout and retries guard robustness.

---

## 7) Concurrency, Backpressure, and Memory
- Orchestration Semaphores
    - domainSemaphore (maxConcurrentDomains, fair).
    - groupSemaphore (maxConcurrentGroups, fair).
    - JobExecutor per-group semaphore (caps concurrent groups handled by the executor).
- Service-Level Concurrency
    - CPU gating via cpuTaskSemaphore (~2×CPU).
    - IO parallelism via ioExecutor for streaming and storage writes.
- Backpressure / Memory Guards
    - Streaming fetchSize (BATCH_SIZE_FROM_CURSOR).
    - Dynamic sub-batching based on live memory (MEMORY_THRESHOLD_RATIO).
    - NODES_PER_PROCESSING_BATCH and MAX_NODES_PER_BATCH cap per-page load.
    - maxMatchesPerNode caps adjacency queues.
    - On memoryExceeded: cancel pending futures and stop CPU execution to protect process.

---

## 8) Matching Strategies (Pluggable)
- AuctionApproximateMatchingStrategy
    - Deduplicates & sorts candidates per left node; maintains prices on right nodes; competitive bidding; fallback for unmatched.
    - Produces near-optimal one-to-one assignments.
- HopcroftKarpMatchingStrategy
    - Creates bipartite graph from nodes present in potential matches; invokes BipartiteGraphBuilderService; runs HK max matching.
- HungarianMatchingStrategy
    - Builds rectangular cost matrix (negative weights for maximization) from bipartite graph edges; runs Hungarian algorithm for optimal assignment.
- TopKWeightedGreedyMatchingStrategy
    - Memory-aware streaming; maintains top‑K per node using PQ; no global one-to-one constraint (best‑K choices per node).

Strategy selection is externalized via MatchingConfiguration. The module simply resolves and executes.

---

## 9) Resilience, Timeouts, Retries
- Scheduler per-group execution wrapped with @Retry/@CircuitBreaker (fallback marks lastRun FAILED).
- JobExecutor retries group processing with exponential backoff; metrics on exhausted retries.
- Streaming service retries SQL errors up to 3 attempts with backoff.
- Timeouts:
    - Page processing per CPU task.
    - Save matches timeout (PerfectMatchSaver and PerfectMatchStorageProcessor).
- Storage @Retryable:
    - Batch save retries SQL/Timeout exceptions with exponential backoff.
- State tracking:
    - LastRunPerfectMatches ensures idempotent re-runs and visibility into status/progress.

---

## 10) Observability
- Metrics
    - Scheduler: perfect_matches_creation timer; perfect_matches_creation_errors_total; fallback counters.
    - Matching: matching_duration; matching_errors_total; stream_records_processed_total; adjacency_map_current_size gauge.
    - Storage: perfect_match_storage_duration; perfect_match_storage_batch_duration; perfect_match_storage_*_total; errors by type.
    - Concurrency: semaphore_acquire_timeout; retry_attempts_total; group_processing_errors.
- Logging
    - Consistent tagging with groupId, domainId, cycleId.
    - Detailed logs on batch sizes, memory adjustments, retries, fallbacks.

---

## 11) Configuration (Selected)
- Scheduling/concurrency
    - Cron: 0 0 3 * * * (Asia/Kolkata)
    - match.max-concurrent-domains (default 2)
    - match.max-concurrent-groups (default 1)
- Retries/timeouts
    - match.max-retries (default 3)
    - match.retry-delay-millis (default 1000)
    - PAGE_PROCESSING_TIMEOUT_SECONDS (default 300)
    - SAVE_MATCHES_TIMEOUT_SECONDS (default 600)
    - PerfectMatchSaver timeout (30 minutes)
    - PerfectMatchStorageProcessor SAVE_OPERATION_TIMEOUT_MS (30 minutes)
- Streaming/batching/memory
    - BATCH_SIZE_FROM_CURSOR (default 5000)
    - NODES_PER_PROCESSING_BATCH (default 100)
    - MAX_NODES_PER_BATCH (default 10000)
    - matching.topk.count (default 100)
    - matching.max.memory.mb (default 1024), MEMORY_THRESHOLD_RATIO (0.8)
    - import.batch-size for perfect match persistence (default 1000)
- Strategy-specific
    - Auction: matching.maxIterations (default 10000)
    - Greedy: matching.max.distinct.nodes, matching.parallelism.level

---

## 12) Key Sequences

### 12.1 Scheduled Cycle
```
PerfectMatchesCreationScheduler
  → getTasksToProcess()
  → for each (domain, group):
       set lastRun PENDING; cache.clear(group)
       acquire domain & group semaphores
       PerfectMatchCreationJobExecutor.processGroup (with retries)
         → PerfectMatchServiceImpl.processAndSaveMatches
           → build context (configuration + match type)
           → streamAllMatches (fetchSize)
              → for each streamed batch:
                   - acquire cpuTaskSemaphore
                   - dynamic sub-batch
                   - per-node top‑K adjacency
                   - strategy.match(...)
                   - PerfectMatchSaver.saveMatchesAsync (batched)
           → wait all batches; metrics/logs
       update lastRun (COMPLETED/FAILED + nodeCount)
       release semaphores
  → MatchesCreationFinalizer.finalize(true)
```

---

## 13) Data and Entities
- PotentialMatchEntity (input):
    - group_id, domain_id, reference_id, matched_reference_id, compatibility_score
- PerfectMatchEntity (output):
    - id, group_id, domain_id, processing_cycle_id, reference_id, matched_reference_id, compatibility_score, matched_at
- LastRunPerfectMatches:
    - groupId, domainId, runDate, status (PENDING/COMPLETED/FAILED), nodeCount
- MatchingConfiguration / MatchingGroup / MatchingAlgorithm:
    - Drive strategy selection and behavior

---

## 14) Risks and Considerations
- Memory safety:
    - On memoryExceeded, CPU executor is shutdownNow in service; ensure it’s scoped appropriately or reinitialized before the next run.
- Concurrency layering:
    - PerfectMatchCreationService uses groupSemaphore while JobExecutor also uses per-group semaphores—verify combined limits meet throughput goals.
- Saver semaphore:
    - PerfectMatchSaver declares a saveSemaphore but does not acquire it; either remove or enforce if needed for backpressure.
- Strategy beans:
    - Ensure unique bean names and single registration for each strategy (avoid duplicates).
- Flush loop:
    - Perfect module intentionally does not use QueueManager/MapDB; doFlushLoop is a placeholder by design.

---

## 15) Extensibility
- Add new matching algorithms by implementing MatchingStrategy and registering in Spring (strategyMap).
- Control behavior via DB MatchingConfiguration without code changes.
- Tune batch sizes, timeouts, and memory limits via configuration.
- Swap streaming source or persistence schema with limited changes to service and storage processor.

---

This HLD reflects the complete scheduled perfect matches creation module, including the PerfectMatchSaver and PerfectMatchStorageProcessor persistence layer.