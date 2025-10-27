# Scheduled Perfect Matches Creation — Low‑Level Design (LLD)

## Table of Contents
- [1. Modules and Relationships](#1-modules-and-relationships)
- [2. Runtime Topology (Threads, Semaphores, Concurrency)](#2-runtime-topology-threads-semaphores-concurrency)
- [3. Detailed Component Specs](#3-detailed-component-specs)
    - [3.1 PerfectMatchesCreationScheduler](#31-perfectmatchescreationscheduler)
    - [3.2 PerfectMatchCreationService](#32-perfectmatchcreationservice)
    - [3.3 PerfectMatchCreationJobExecutor](#33-perfectmatchcreationjobexecutor)
    - [3.4 PerfectMatchServiceImpl](#34-perfectmatchserviceimpl)
    - [3.5 PotentialMatchStreamingService](#35-potentialmatchstreamingservice)
    - [3.6 MatchingStrategySelector](#36-matchingstrategyselector)
    - [3.7 Strategies](#37-strategies)
    - [3.8 PerfectMatchSaver](#38-perfectmatchsaver)
    - [3.9 PerfectMatchStorageProcessor](#39-perfectmatchstorageprocessor)
    - [3.10 GraphPreProcessor.determineMatchType](#310-graphpreprocessordeterminematchtype)
- [4. Data Model](#4-data-model)
- [5. Key Algorithms & Pseudocode](#5-key-algorithms--pseudocode)
- [6. Error Handling, Timeouts, Retries](#6-error-handling-timeouts-retries)
- [7. Metrics and Logging](#7-metrics-and-logging)
- [8. Configuration Matrix](#8-configuration-matrix)
- [9. Execution Sequences](#9-execution-sequences)
- [10. External Contracts and Assumptions](#10-external-contracts-and-assumptions)
- [11. Risks, Nuances, Recommendations](#11-risks-nuances-recommendations)
- [12. Testing Strategy](#12-testing-strategy)

---

## 1. Modules and Relationships
- Orchestration
    - PerfectMatchesCreationScheduler → PerfectMatchCreationService → PerfectMatchCreationJobExecutor
- Matching pipeline
    - PerfectMatchServiceImpl → MatchingStrategySelector → MatchingStrategy
- Data access and persistence
    - PotentialMatchStreamingService (read PotentialMatchEntity stream)
    - PerfectMatchSaver → PerfectMatchStorageProcessor (save PerfectMatchEntity)
- Utilities/Supporting
    - GraphPreProcessor.determineMatchType (type inference)
    - MatchesCreationFinalizer (signal end-of-batch)
    - MatchCache.clearMatches(groupId) at start of per-group run

---

## 2. Runtime Topology (Threads, Semaphores, Concurrency)
- Executors
    - matchCreationExecutorService: orchestration, job executor callbacks
    - ioExecutorService: JDBC streaming and storage writer
    - cpuExecutor: CPU-bound sub-batch processing and strategy execution
- Semaphores
    - PerfectMatchCreationService: domainSemaphore (fair), groupSemaphore (fair)
    - PerfectMatchCreationJobExecutor: per-group semaphore map (maxConcurrentGroups)
    - PerfectMatchServiceImpl: cpuTaskSemaphore ≈ 2 × availableProcessors
- Acquisition order to avoid deadlocks
    1) domainSemaphore
    2) groupSemaphore (service level)
    3) JobExecutor groupSemaphore (per-group map)
    4) cpuTaskSemaphore (per streamed batch)

Backpressure and memory
- Stream fetch size (BATCH_SIZE_FROM_CURSOR)
- Dynamic sub-batch sizing (based on live memory usage vs matching.max.memory.mb × threshold)
- NODES_PER_PROCESSING_BATCH, MAX_NODES_PER_BATCH, maxMatchesPerNode

---

## 3. Detailed Component Specs

### 3.1 PerfectMatchesCreationScheduler
- Schedule: @Scheduled(cron = "0 0 3 * * *", zone = "Asia/Kolkata")
- Flow:
    - Start timer perfect_matches_creation
    - tasks = perfectMatchCreationService.getTasksToProcess()
    - For each task (domain, group): generatePerfectMatchesCreationGroup(groupId, domainId)
        - @Retry/@CircuitBreaker guarded
        - Inside: update LastRun to PENDING; clear cache; perfectMatchCreationService.processAllDomains(); update LastRun to COMPLETED with processed count
- Fallback: marks lastRun FAILED
- Metrics:
    - perfect_matches_creation timer
    - perfect_matches_creation_errors_total
    - perfect_matches_creation_fallback

Note: generatePerfectMatchesCreationGroup calls processAllDomains (processes all eligible groups); scheduler also iterates tasks — see [11].

### 3.2 PerfectMatchCreationService
- Responsibilities
    - Discover tasks: For active domains and their groups, compute shouldProcess:
        - processedNodes > lastRun.nodeCount OR lastRun.status ∈ {PENDING, FAILED, null}
    - Maintain LastRunPerfectMatches (read/write)
    - Concurrency: domainSemaphore(maxConcurrentDomains), groupSemaphore(maxConcurrentGroups)
    - Orchestrate batch (“perfect-match batch”) for all eligible tasks
- processAllDomains:
    - New cycleId; build task list; submit processGroupTask for each
    - Wait all, then MatchesCreationFinalizer.finalize(true)
- processGroupTask(groupId, domainId, cycleId)
    - LastRun := PENDING with runDate=now
    - Acquire domainSemaphore (3 min) → groupSemaphore (3 min)
    - jobExecutor.processGroup(...).thenRunAsync:
        - doFlushLoop (currently a no-op placeholder)
        - Update LastRun to COMPLETED with current processedNodes
    - Release semaphores; on exception: counter matches_creation_error (mode="perfect"), LastRun := FAILED
- Metrics:
    - batch_perfect_matches_total_duration (batch)
    - batch_perfect_matches_duration (per group)
    - task_processing_duration (per group)

### 3.3 PerfectMatchCreationJobExecutor
- processGroup(groupId, domainId, cycleId) → CompletableFuture<Void>
    - Per-group semaphore (groupSemaphores map, permits=maxConcurrentGroups); acquire (60s)
    - processGroupWithRetriesAsync(...):
        - Recursive retries up to maxRetries; backoff retryDelayMillis × 2^(attempt - 1)
        - Delegates to perfectMatchService.processAndSaveMatches(request)
    - Release per-group semaphore; remove map entry when idle
- Metrics:
    - semaphore_acquire_timeout (matchType="perfect")
    - group_processing_errors, max_retries_exceeded, retry_attempts_total

### 3.4 PerfectMatchServiceImpl
- Dependencies: PotentialMatchStreamingService, PerfectMatchSaver, MatchingStrategySelector, MatchingConfigurationRepository, GraphPreProcessor, ioExecutor, cpuExecutor
- Key config:
    - maxMatchesPerNode (matching.topk.count)
    - maxMemoryMb (matching.max.memory.mb)
    - Constants: PAGE_PROCESSING_TIMEOUT_SECONDS=300, SAVE_MATCHES_TIMEOUT_SECONDS=600, NODES_PER_PROCESSING_BATCH=100, MAX_NODES_PER_BATCH=10000, BASE_SUB_BATCH_SIZE=500, MEMORY_THRESHOLD_RATIO=0.8
- Process:
    - processAndSaveMatches(request)
        - Build context:
            - Fetch MatchingConfiguration by groupId, domainId
            - Determine match type via graphPreProcessor.determineMatchType
            - GraphRequestFactory.buildMatchingContext(...)
        - thenCompose → processMatchesWithCursor(context, groupId, domainId, cycleId)
        - When complete: stop matching_duration; increment matching_errors_total on failure
    - processMatchesWithCursor(context, groupId, domainId, cycleId)
        - Select MatchingStrategy via MatchingStrategySelector.select
        - Stream potential matches via PotentialMatchStreamingService.streamAllMatches(groupId, domainId, batchConsumer, BATCH_SIZE_FROM_CURSOR)
        - batchConsumer (executed per streamed batch):
            - Acquire cpuTaskSemaphore
            - For each subBatch (size determined by adjustBatchSize()):
                - Build grouped map of nodes appearing in subBatch (by referenceId and matchedReferenceId)
                - While nodes remain and nodesProcessed < NODES_PER_PROCESSING_BATCH:
                    - Build priority queue (size maxMatchesPerNode) with top-K potential matches per node (both directions)
                    - Add PQ contents to nodeMatches list
                    - If memory threshold exceeded → memoryExceeded=true, cancel futures, cpuExecutor.shutdownNow, throw
                    - Periodically, aggregate nodeMatches into nodeMap: Map<String, PriorityQueue<PotentialMatch>> with top-K per node; call processPageMatches(nodeMap, strategy,...); clear nodeMatches
            - Release cpuTaskSemaphore
            - orTimeout(PAGE_PROCESSING_TIMEOUT_SECONDS)
        - Wait all page futures; on error: cancel, cpuExecutor.shutdownNow
    - processPageMatches(nodeMap, strategy, groupId, domainId, cycleId)
        - Build flat list of PotentialMatch from nodeMap
        - strategy.match(...) → Map<String, List<MatchResult>>
        - Build PerfectMatchEntity buffer; for every buffer of size ≥ maxMatchesPerNode → perfectMatchSaver.saveMatchesAsync(...).orTimeout(SAVE_MATCHES_TIMEOUT_SECONDS)
        - Save remainder; return allOf(saveFutures)
- Memory safety helpers:
    - adjustBatchSize(): adapt based on current memory usage vs threshold
    - isMemoryThresholdExceeded(): returns true if usedMb > maxMemoryMb × threshold

### 3.5 PotentialMatchStreamingService
- streamAllMatches(groupId, domainId, batchConsumer, batchSize)
    - JDBC (forward-only, read-only) with fetch size
    - Reads rows: (reference_id, matched_reference_id, compatibility_score, group_id, domain_id)
    - Validates non-null IDs; collects into buffer of batchSize; calls batchConsumer on each buffer
    - Retries up to 3 times on SQLException with incremental sleep (1s × retryCount)
    - Commits at the end
- streamMatches(groupId, domainId, offset, limit) [JPA Stream]
    - Used for sampling (e.g., match type inference)

### 3.6 MatchingStrategySelector
- select(context, groupId)
    - Loads MatchingGroup by domainId+groupId
    - Loads MatchingConfiguration for group
    - strategyMap lookup by algorithm id/name; throws BadRequest if not found

### 3.7 Strategies
- Common interface:
    - match(List<GraphRecords.PotentialMatch> allPMs, UUID groupId, UUID domainId) → Map<String, List<MatchResult>>
    - supports(String mode)
- AuctionApproximateMatchingStrategy
    - Deduplicate per-left by matchedReferenceId; sort by score desc; auction loop with right-node prices; fallback allocation for losers; constraints: [0,1] score range, same groupId/domainId only
- HopcroftKarpMatchingStrategy
    - Build unique left/right nodes from PotentialMatch list
    - bipartiteGraphBuilderService.build(left, right, request) → Graph
    - Construct BipartiteGraph; run HopcroftKarpAlgorithm; produce 1:1 mapping
- HungarianMatchingStrategy
    - Build left/right sets, call builder to get Graph
    - Build rectangular costMatrix with -weight; run Hungarian algorithm; map to results
- TopKWeightedGreedyMatchingStrategy
    - Memory-aware pipeline; partition into sub-batches (dynamic)
    - Build adjacencyMap: Map<String, PriorityQueue<PotentialMatch>> for both directions (symmetrized)
    - Keep top-K per node (bounded PQ), guard with maxDistinctNodes; compute results by draining PQ into MatchResult list
    - Parallelized per-node processing with computeSemaphore

### 3.8 PerfectMatchSaver
- saveMatchesAsync(matches, groupId, domainId, processingCycleId) → CompletableFuture<Void>
    - Delegates to PerfectMatchStorageProcessor.savePerfectMatches
    - Global timeout: 30 minutes
    - Shutdown guard; logs and fails fast if shutting down
- Note: saveSemaphore is declared but not used (see [11])

### 3.9 PerfectMatchStorageProcessor
- savePerfectMatches(matches, groupId, domainId, cycleId) → CF<Void>
    - If empty: complete immediately
    - Wraps saveInBatches on ioExecutor; timeout SAVE_OPERATION_TIMEOUT_MS (30 min)
    - On completion: perfect_match_storage_duration timer; increment saved or error counters
- saveInBatches(matches, groupId, domainId, cycleId)
    - Partition by import.batch-size (default 1000); for each batch → saveBatch
- saveBatch(batch, groupId, domainId, cycleId) [@Transactional, @Retryable]
    - Connection autoCommit=false; synchronous_commit=OFF
    - Create temp table via QueryUtils.getPrefectMatchesTempTableSQL()
    - COPY BINARY into temp_perfect_matches using PerfectMatchSerializer(binary stream)
    - Upsert into final table via QueryUtils.getUpsertPerfectMatchesSql()
    - Commit; timers and counters per batch

### 3.10 GraphPreProcessor.determineMatchType
- Streams one PotentialMatchEntity for group/domain
- Loads corresponding Nodes; if both exist and types equal → SYMMETRIC; else BIPARTITE
- On any exception or missing info → default BIPARTITE

---

## 4. Data Model
- PotentialMatchEntity (input)
    - group_id (UUID), domain_id (UUID), reference_id (String), matched_reference_id (String), compatibility_score (double), matched_at (timestamp)
- GraphRecords.PotentialMatch (in-memory DTO)
    - referenceId (String), matchedReferenceId (String), compatibilityScore (double), groupId (UUID), domainId (UUID)
- PerfectMatchEntity (output)
    - id (UUID), group_id (UUID), domain_id (UUID), processing_cycle_id (String), reference_id (String), matched_reference_id (String), compatibility_score (double), matched_at (timestamp)
- LastRunPerfectMatches
    - groupId, domainId, runDate, status (PENDING/COMPLETED/FAILED), nodeCount

Indexes (recommended):
- perfect_matches unique (group_id, reference_id, matched_reference_id)
- perfect_matches idx (group_id, domain_id, processing_cycle_id)

---

## 5. Key Algorithms & Pseudocode

- Scheduler cycle
```
createPerfectMatches():
  tasks = service.getTasksToProcess()
  for each (domain, group):
    try:
      generatePerfectMatchesCreationGroup(groupId, domainId)  // @Retry/@CB
    catch:
      increment fallback metrics; mark lastRun FAILED
```

- Service group task
```
processGroupTask(groupId, domainId, cycleId):
  lastRun := PENDING(now); save
  acquire domainSemaphore(3m)
  acquire groupSemaphore(3m)
  try:
    jobExecutor.processGroup(...).thenRunAsync(() -> {
      doFlushLoop() // no-op currently
      lastRun := COMPLETED with nodeCount := count processed nodes; save
    })
  finally:
    release semaphores
    on error: counters + lastRun := FAILED
```

- Job executor retries
```
processGroupWithRetriesRecursive(..., attempt):
  if attempt > maxRetries: metrics + completeExceptionally
  else:
    perfectMatchService.processAndSaveMatches(request)
      .whenCompleteAsync((ok, t) -> {
         if t != null:
           sleep(retryDelayMillis * 2^(attempt-1))
           recurse(attempt+1)
         else complete
      })
```

- PerfectMatchServiceImpl streaming and batching
```
processAndSaveMatches(request):
  ctx = buildContextFromConfigAndMatchType()
  return processMatchesWithCursor(ctx, groupId, domainId, cycleId)

processMatchesWithCursor(ctx, gid, did, cycleId):
  strategy = select(ctx, gid)
  pageFutures = []
  try:
    streamAllMatches(gid, did, batchConsumer, BATCH_SIZE_FROM_CURSOR)
    batchConsumer(batch):
      acquire cpuTaskSemaphore
      runAsync on cpuExecutor (timeout PAGE_PROCESSING_TIMEOUT_SECONDS):
        for each subBatch in partition(batch, adjustBatchSize()):
          grouped = group entries by node id (both refId and matchedRefId)
          nodeMatches = []
          nodesProcessed=0
          for each node in grouped while nodesProcessed < NODES_PER_PROCESSING_BATCH:
            if memoryExceeded or interrupted -> cancel & throw
            queue = PQ(topK by score)
            enqueue both directions for node
            nodeMatches += queue; nodesProcessed++
            periodically:
              nodeMap = toMapOfPQ(nodeMatches)
              processPageMatches(nodeMap, strategy, gid, did, cycleId)
              nodeMatches.clear()
      release cpuTaskSemaphore
    pageFutures.add(...)
  finally:
    await all pageFutures; on fail -> cancel others, cpuExecutor.shutdownNow
```

- processPageMatches
```
nodeMap -> flatPotentialMatches
batchResult = strategy.match(flatPotentialMatches, gid, did)
buffer = []
for each (nodeId -> list<MatchResult>) in batchResult:
  for each match in list:
    buffer.add(PerfectMatchEntity(...))
    if buffer.size >= maxMatchesPerNode:
      save(buffer); meter + log; buffer.clear()
if buffer not empty: save(buffer)
return allOf(saveFutures)
```

- PotentialMatchStorageProcessor.saveBatch
```
conn.setAutoCommit(false)
SET synchronous_commit = OFF
create temp table (QueryUtils.getPrefectMatchesTempTableSQL)
COPY BINARY temp_perfect_matches FROM STDIN (BinaryCopyInputStream over PerfectMatchSerializer)
PreparedStatement(QueryUtils.getUpsertPerfectMatchesSql).executeUpdate()
conn.commit()
```

---

## 6. Error Handling, Timeouts, Retries
- Scheduler/Service
    - @Retry and @CircuitBreaker per group run; fallback updates LastRun to FAILED
    - Semaphore tryAcquire with bounded timeouts; on failure -> RuntimeException to bubble up
- JobExecutor
    - Retries with exponential backoff; counters for attempts and exhaustion
- PerfectMatchServiceImpl
    - Page CPU tasks orTimeout (PAGE_PROCESSING_TIMEOUT_SECONDS)
    - Save orTimeout (SAVE_MATCHES_TIMEOUT_SECONDS)
    - memoryExceeded → cancel tasks + shutdownNow CPU executor
- Streaming
    - Up to 3 retries on SQLException with incremental backoff; throws if exceeds
- Storage
    - Top-level save timeout SAVE_OPERATION_TIMEOUT_MS (30 min)
    - @Retryable saveBatch: SQLException, TimeoutException, up to 3 attempts with exponential backoff
- Fallbacks
    - Scheduler fallback increments counters and sets lastRun FAILED

---

## 7. Metrics and Logging
- Timers
    - perfect_matches_creation (scheduler scope)
    - batch_perfect_matches_total_duration, batch_perfect_matches_duration, task_processing_duration
    - matching_duration
    - perfect_match_storage_duration, perfect_match_storage_batch_duration
- Counters
    - perfect_matches_creation_errors_total, perfect_matches_creation_fallback
    - group_processing_errors, retry_attempts_total, max_retries_exceeded
    - matching_errors_total, stream_records_processed_total
    - perfect_matches_saved_total (service level)
    - perfect_match_storage_matches_saved_total, perfect_match_storage_batch_processed_total
    - perfect_match_storage_errors_total (by error_type)
    - semaphore_acquire_timeout (matchType="perfect")
- Gauges
    - adjacency_map_current_size
    - system_cpu_usage (OS bean)
- Logging
    - Consistent tags: groupId, domainId, cycleId
    - Memory adjustments, retries/backoff, batch sizes, save successes/failures

---

## 8. Configuration Matrix
| Key | Default | Component | Effect |
|---|---:|---|---|
| Cron | 0 0 3 * * * | Scheduler | Daily trigger (Asia/Kolkata) |
| match.max-concurrent-domains | 2 | Service | Domain-level parallelism |
| match.max-concurrent-groups | 1 | Service/JobExecutor | Parallel groups (service-level and per-group map) |
| match.max-retries | 3 | JobExecutor | Group retries |
| match.retry-delay-millis | 1000 | JobExecutor | Backoff base |
| matching.topk.count | 100 | Service/Strategies | Top‑K matches per node |
| matching.max.memory.mb | 1024 | Service/Greedy | Memory ceiling for dynamic batching |
| PAGE_PROCESSING_TIMEOUT_SECONDS | 300 | Service | CPU batch timeout |
| SAVE_MATCHES_TIMEOUT_SECONDS | 600 | Service | Save timeout per batch |
| BATCH_SIZE_FROM_CURSOR | 5000 | Streaming | JDBC fetch size |
| MAX_NODES_PER_BATCH | 10000 | Service | Hard limit per page |
| NODES_PER_PROCESSING_BATCH | 100 | Service | Node processing window per page |
| import.batch-size | 1000 | Storage | COPY/upsert batch size |
| SAVE_OPERATION_TIMEOUT_MS | 1_800_000 | Storage | Top-level save timeout |
| matching.maxIterations | 10000 | Auction | Max auction iterations |
| matching.max.distinct.nodes | 10000 | Greedy | Distinct node cap |
| matching.parallelism.level | 0 (auto) | Greedy | Compute semaphore parallelism |

---

## 9. Execution Sequences

### 9.1 Scheduled Batch
```
Scheduler
  → Service.getTasksToProcess
  → for each (domain, group):
       generatePerfectMatchesCreationGroup
         - lastRun := PENDING; cache.clear
         - Service.processAllDomains:
             - for each task → processGroupTask
               - acquire domain & group semaphores
               - JobExecutor.processGroup (with retries)
                 → PerfectMatchServiceImpl.processAndSaveMatches
               - update lastRun := COMPLETED + nodeCount
               - release semaphores
  → MatchesCreationFinalizer.finalize(true)
```

### 9.2 PerfectMatchServiceImpl (per streamed batch)
```
streamAllMatches(batchSize):
  on batch → cpuExecutor.runAsync:
    acquire cpuTaskSemaphore
    for each subBatch in partition(batch, adjustBatchSize()):
      groupPotentialMatchesByNode()
      while nodesProcessed < NODES_PER_PROCESSING_BATCH:
        PQ := top-K by score for node (both directions)
        nodeMatches += PQ
        if memoryThresholdExceeded(): cancel & shutdownNow
        periodically:
          nodeMap := aggregate top-K PQs
          processPageMatches(nodeMap, strategy) → save PerfectMatchEntity in batches
    release cpuTaskSemaphore
```

### 9.3 Storage (per batch)
```
saveBatch():
  begin tx
  SET synchronous_commit = OFF
  CREATE TEMP TABLE temp_perfect_matches
  COPY BINARY into temp table
  UPSERT via QueryUtils.getUpsertPerfectMatchesSql()
  COMMIT
```

---

## 10. External Contracts and Assumptions
- DomainService.getActiveDomains()
- MatchingGroupRepository
    - findGroupIdsByDomainId(domainId)
    - findByDomainIdAndId(domainId, groupId)
- LastRunPerfectMatchesRepository.findByDomainIdAndGroupId / save
- NodeRepository.countByDomainIdAndGroupIdAndProcessedTrue
- MatchingConfigurationRepository.findByGroupIdAndDomainId / findByGroup
- MatchingAlgorithmRepository (seeding)
- GraphPreProcessor.determineMatchType
- QueryUtils
    - getAllPotentialMatchesStreamingSQL()
    - getPotentialMatchesStreamingSQL() [JPA query string]
    - getPrefectMatchesTempTableSQL()
    - getUpsertPerfectMatchesSql()
- GraphRequestFactory.buildMatchingContext(...)
- PerfectMatchEntity / PotentialMatchEntity serializers:
    - PerfectMatchSerializer for COPY BINARY
    - BinaryCopyInputStream

---

## 11. Risks, Nuances, Recommendations
- Duplicate orchestration execution
    - Scheduler iterates tasks and calls generatePerfectMatchesCreationGroup, which internally invokes processAllDomains (processing all tasks again). Consider invoking processAllDomains only once per schedule.
- Dual group semaphores
    - Service-level groupSemaphore and JobExecutor per-group semaphore both throttle concurrency. Ensure configured limits reflect desired throughput (may be over-constrained).
- Memory abort behavior
    - On memoryExceeded, cpuExecutor.shutdownNow() is invoked. Ensure executor is dedicated to this module or recreated before the next run to avoid starvation.
- PerfectMatchSaver.saveSemaphore
    - Declared but unused; either remove or enforce for write backpressure.
- Strategy bean uniqueness
    - Ensure unique bean names; avoid duplicate @Component("auctionApproximateMatchingStrategy") definitions.
- Validation
    - Strategies filter by groupId/domainId; ensure all PotentialMatch DTOs carry correct identifiers.
- Observability tag consistency
    - Prefer consistent tags across counters/timers (groupId/domainId/cycleId/matchType).

---

## 12. Testing Strategy
- Unit
    - MatchingStrategySelector selection and error paths
    - AuctionApproximateMatchingStrategy: dedupe, price updates, fallback
    - Hungarian & Hopcroft‑Karp: small graphs with known optima
    - TopKWeightedGreedy: PQ top‑K correctness; memory guard paths
    - PerfectMatchStorageProcessor.saveBatch: verifies COPY + upsert (use test DB)
    - PotentialMatchStreamingService: streaming boundaries, retry on SQLException
- Integration
    - End-to-end per group: stream→strategy→save; verify PerfectMatchEntity rows and LastRun transitions
    - Retry behavior: induce failures in streaming and storage to validate backoff and counters
    - Concurrency: multiple groups/domains with semaphores; ensure no deadlocks
- Performance
    - Large PotentialMatch streams; tune fetchSize, batch sizes; observe memory thresholds
    - Storage throughput under concurrent saves; measure perfect_match_storage_* timers
- Failover/Resilience
    - Simulate memoryExceeded and ensure graceful cancellation and logging
    - Kill executors mid-run; ensure scheduler still marks failures and subsequent cycles recover

---

This LLD documents the components, execution model, data flow, algorithms, concurrency, and persistence details for the Scheduled Perfect Matches Creation module, including the saver/storage additions.