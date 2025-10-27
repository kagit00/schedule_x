# Match Transfer to Client — Low‑Level Design (LLD)

## Table of Contents
- [1. Modules and Relationships](#1-modules-and-relationships)
- [2. Runtime Topology (Executors, Queues, Concurrency)](#2-runtime-topology-executors-queues-concurrency)
- [3. Detailed Component Specs](#3-detailed-component-specs)
    - [3.1 MatchesTransferScheduler](#31-matchestransferscheduler)
    - [3.2 MatchTransferService](#32-matchtransferservice)
    - [3.3 MatchTransferProcessor](#33-matchtransferprocessor)
    - [3.4 PotentialMatchStreamingService](#34-potentialmatchstreamingservice)
    - [3.5 PerfectMatchStreamingService](#35-perfectmatchstreamingservice)
- [4. Data Model](#4-data-model)
- [5. Pipeline Algorithms & Pseudocode](#5-pipeline-algorithms--pseudocode)
- [6. Error Handling, Timeouts, Retries](#6-error-handling-timeouts-retries)
- [7. Metrics and Logging](#7-metrics-and-logging)
- [8. Configuration Matrix](#8-configuration-matrix)
- [9. Execution Sequences](#9-execution-sequences)
- [10. External Contracts and Assumptions](#10-external-contracts-and-assumptions)
- [11. Risks, Nuances, Recommendations](#11-risks-nuances-recommendations)
- [12. Testing Strategy](#12-testing-strategy)

---

## 1. Modules and Relationships
- MatchesTransferScheduler (cron) → submits per-group transfer tasks
- MatchTransferService → thin wrapper delegating to processor
- MatchTransferProcessor → orchestrates streaming (potential + perfect) → transforms → exports → publishes
- PotentialMatchStreamingService / PerfectMatchStreamingService → JDBC streaming from DB
- ExportService → creates export artifact (file/object) from a Stream<MatchTransfer>
- ScheduleXProducer → publishes file reference to messaging topic

---

## 2. Runtime Topology (Executors, Queues, Concurrency)
- Executors
    - matchTransferGroupExecutor (ThreadPoolTaskExecutor): runs one async task per group
    - matchTransferExecutor (ThreadPoolTaskExecutor): runs producers (streamers) and consumer (export) within a group
- Bounded buffer
    - LinkedBlockingQueue<List<MatchTransfer>> capacity=100 (each list ≈ batchSize)
    - Producers block on queue.put; consumer polls queue with timeout (poll 300ms)
- Producer threads
    - 2 producers per group: PotentialMatch stream + PerfectMatch stream
- Consumer thread
    - Export + send (exportAndSend) runs on matchTransferExecutor, consuming the queue via a lazy Stream supplier
- Termination coordination
    - AtomicBoolean done set when both producers complete; consumer stops when done and queue is empty

---

## 3. Detailed Component Specs

### 3.1 MatchesTransferScheduler
- Dependencies: MatchTransferService, MatchingGroupRepository, DomainService, Executor matchTransferGroupExecutor, MeterRegistry
- scheduledMatchesTransferJob()
    - domainService.getActiveDomains(); warn and return if empty
    - Register gauges on matchTransferGroupExecutor (active threads, queue size; warn on high queue depth)
    - For each domain:
        - groupIds = matchingGroupRepository.findGroupIdsByDomainId(domainId)
        - For each groupId:
            - Timer.Sample sample
            - CompletableFuture.runAsync(() -> matchTransferService.processGroup(groupId, domain), matchTransferGroupExecutor)
              .whenComplete -> on failure: counter group_process_failed++; stop timer group_process_duration
- Notes
    - Fire-and-forget of per-group CFs; scheduler does not await completion
    - Logs: “Starting matching for domain={name} with groups: {ids}”

### 3.2 MatchTransferService
- processGroup(UUID groupId, Domain domain)
    - Delegates to matchTransferProcessor.processMatchTransfer(groupId, domain)

### 3.3 MatchTransferProcessor
- Constants: MATCH_EXPORT_TOPIC = "matches-suggestions"
- Dependencies: PotentialMatchStreamingService, PerfectMatchStreamingService, ExportService, ScheduleXProducer, MeterRegistry, Executor matchTransferExecutor
- Config: @Value match.transfer.batch-size (default 100000)
- processMatchTransfer(UUID groupId, Domain domain) → CompletableFuture<Void>
    - Annotations: @CircuitBreaker(name="matchProcessor", fallbackMethod="processMatchTransferFallback")
    - Register gauges on matchTransferExecutor (active threads, queue size; warn on high depth)
    - Timer.Sample sample (match_process_duration)
    - Pipeline:
        - supplyAsync (on matchTransferExecutor) returns CF<CF<Void>>:
            - recordCount=0, queue=LinkedBlockingQueue<>(100), done=false
            - potentialConsumer: List<PotentialMatchEntity> → map ResponseMakerUtility.buildMatchTransfer → queue.put; counter match_export_batch_count{type=potential}
            - perfectConsumer: List<PerfectMatchEntity> → map → queue.put; counter match_export_batch_count{type=perfect}
            - potentialFuture = runAsync(streamPotential(...), matchTransferExecutor)
            - perfectFuture = runAsync(streamPerfect(...), matchTransferExecutor)
            - bothDone = allOf(potentialFuture, perfectFuture); bothDone.whenComplete((v,t) -> done=true)
            - matchesSupplier = Supplier<Stream<MatchTransfer>> using:
                - Stream.generate(() -> queue.poll(300ms))
                  .takeWhile(batch -> !(done && (batch==null || batch.isEmpty())))
                  .filter(Objects::nonNull)
                  .flatMap(Collection::stream)
                  .onClose(() -> done=true)
            - return exportAndSend(groupId, domain, matchesSupplier)
              .whenComplete -> on success: record counter match_export_records += recordCount
        - thenCompose to flatten CF<CF<Void>> → CF<Void>
        - whenComplete -> on failure: counter match_process_failed++; stop timer
        - exceptionally -> same counter and rethrow RuntimeException
- exportAndSendSync(UUID groupId, Domain domain, Supplier<Stream<MatchTransfer>> matchesSupplier)
    - Annotations: @Retryable(value={ConnectException, TimeoutException}, maxAttempts=3, backoff=100ms×2^n)
    - Timer.Sample sample (export_send_duration)
    - ExportedFile file = exportService.exportMatches(matchesSupplier, groupId, domainId).join()
    - Build MatchSuggestionsExchange payload via GraphRequestFactory.buildFileReference
    - topic = domain.getName().toLowerCase() + "-" + MATCH_EXPORT_TOPIC
    - key   = domainId + "-" + groupId
    - scheduleXProducer.sendMessage(topic, key, BasicUtility.stringifyObject(payload), false)
    - counter match_export_success++; stop timer
- exportAndSend(...) → supplyAsync(() -> { exportAndSendSync(...); return null; }, matchTransferExecutor)
- processMatchTransferFallback(UUID groupId, Domain domain, Throwable t)
    - Counter match_circuit_breaker_tripped++; returns completedFuture(null)

### 3.4 PotentialMatchStreamingService
- streamAllMatches(UUID groupId, UUID domainId, Consumer<List<PotentialMatchEntity>> batchConsumer, int batchSize)
    - Retries: up to 3 on SQLException with incremental backoff (1s × retryCount)
    - Implementation:
        - JDBC Connection (autoCommit=false), PreparedStatement(getAllPotentialMatchesStreamingSQL, FORWARD_ONLY, READ_ONLY), setFetchSize(batchSize)
        - Bind groupId, domainId; executeQuery
        - Iterate ResultSet:
            - Extract columns: reference_id, matched_reference_id, compatibility_score, group_id, domain_id
            - Validate non-null reference_id & matched_reference_id; else warn and continue
            - Fill buffer until size==batchSize → batchConsumer.accept(buffer); reset buffer
        - On remaining buffer → batchConsumer.accept(buffer)
        - Commit connection; return
- streamMatches(groupId, domainId, offset, limit) → JPA Stream (used elsewhere)

### 3.5 PerfectMatchStreamingService
- streamAllMatches(UUID groupId, UUID domainId, Consumer<List<PerfectMatchEntity>> batchConsumer, int batchSize)
    - Identical pattern to PotentialMatchStreamingService using QueryUtils.getAllPerfectMatchesStreamingSQL()

---

## 4. Data Model
- PotentialMatchEntity
    - referenceId, matchedReferenceId, compatibilityScore, groupId, domainId, matchedAt (optional)
- PerfectMatchEntity
    - referenceId, matchedReferenceId, compatibilityScore, groupId, domainId, matchedAt
- MatchTransfer (DTO)
    - Derived from PotentialMatchEntity/PerfectMatchEntity via ResponseMakerUtility.buildMatchTransfer
    - Contains sender/receiver IDs, scores, and any attributes required by client export format
- ExportedFile
    - filePath, fileName, contentType
- MatchSuggestionsExchange
    - Message payload with file reference and identifiers

---

## 5. Pipeline Algorithms & Pseudocode

- processMatchTransfer (core)
```java
recordCount = 0;
queue = new LinkedBlockingQueue<List<MatchTransfer>>(100);
done = new AtomicBoolean(false);

// Producers
potentialFuture = runAsync(() -> streamPotential(groupId, domainId, batch -> {
  transfers = map(batch);
  recordCount += batch.size();
  queue.put(transfers);
}));

perfectFuture = runAsync(() -> streamPerfect(groupId, domainId, batch -> {
  transfers = map(batch);
  recordCount += batch.size();
  queue.put(transfers);
}));

allOf(potentialFuture, perfectFuture).whenComplete((v, t) -> done.set(true));

// Consumer supplier
matchesSupplier = () -> Stream
  .generate(() -> queue.poll(300ms))
  .takeWhile(batch -> !(done.get() && (batch == null || batch.isEmpty())))
  .filter(Objects::nonNull)
  .flatMap(List::stream)
  .onClose(() -> done.set(true));

// Export + Publish
return exportAndSend(groupId, domain, matchesSupplier);
```

- Streaming (per service)
```java
for (retries = 0; retries <= 3; ++retries) {
  try (conn; ps; rs) {
    conn.setAutoCommit(false);
    ps.setFetchSize(batchSize);
    bind(groupId, domainId);
    buffer = new ArrayList<>(batchSize);
    while (rs.next()) {
      if (validRow(rs)) buffer.add(entityFrom(rs));
      if (buffer.size() == batchSize) { batchConsumer.accept(buffer); buffer = new ArrayList<>(batchSize); }
    }
    if (!buffer.isEmpty()) batchConsumer.accept(buffer);
    conn.commit();
    return;
  } catch (SQLException e) {
    if (retries == 3) throw;
    sleep(1000L * (retries + 1));
  }
}
```

- Export + Send
```java
ExportedFile file = exportService.exportMatches(matchesSupplier, groupId, domainId).join();
payload = GraphRequestFactory.buildFileReference(groupId, file.path, file.name, file.contentType, domainId);
topic = (domain.name.toLowerCase() + "-" + "matches-suggestions");
key = domainId + "-" + groupId;
scheduleXProducer.sendMessage(topic, key, stringify(payload), false);
```

---

## 6. Error Handling, Timeouts, Retries
- Circuit breaker
    - processMatchTransfer: trips to fallback (logs, metric; skip transfer)
- Retries
    - Streaming: up to 3 on SQL errors with incremental backoff
    - exportAndSendSync: @Retryable on ConnectException/TimeoutException (3 attempts, exponential backoff)
- Queue operations
    - Producers: queue.put (interruptible); on InterruptedException → restore interrupt and throw RuntimeException
    - Consumer: queue.poll(300ms); continues until done + empty
- CF exception handling
    - whenComplete: mark failure counters; stop timers
    - exceptionally: record failure and wrap in RuntimeException

---

## 7. Metrics and Logging
- Gauges
    - group_executor_active_threads, group_executor_queue_size
    - batch_executor_active_threads, batch_executor_queue_size
- Timers
    - group_process_duration (per group)
    - match_process_duration (per group)
    - export_send_duration (per group)
- Counters
    - group_process_failed, match_process_failed
    - match_export_batch_count{type=potential/perfect}
    - match_export_records (total per group)
    - match_export_success
    - group_executor_queue_high, batch_executor_queue_high (when queue depth crosses warning threshold)
    - match_circuit_breaker_tripped
- Logging
    - Per-domain start, per-group submit, batch sizes streamed, totals exported, exceptions with stack traces

---

## 8. Configuration Matrix
| Key | Default | Component | Effect |
|---|---:|---|---|
| match.transfer.cron-schedule | N/A | Scheduler | Cron schedule for job |
| match.transfer.batch-size | 100000 | Streaming | JDBC fetch size and batch list size |
| matchTransferGroupExecutor | external bean | Scheduler | Controls per-group parallelism |
| matchTransferExecutor | external bean | Processor | Controls producers/consumer concurrency |
| Topic suffix | matches-suggestions | Processor | Final topic name = domain-name-lc + "-" + suffix |

---

## 9. Execution Sequences

### 9.1 Scheduled transfer (per group)
```
Scheduler
  → runAsync MatchTransferService.processGroup(groupId, domain)
    → MatchTransferProcessor.processMatchTransfer
       - start potential stream → map → queue.put
       - start perfect stream   → map → queue.put
       - build lazy stream supplier polling queue
       - exportAndSend (export file → produce message)
       - metrics (counts, timers)
```

### 9.2 Producer/Consumer loop
```
Producer (Potential/Perfect)    Consumer (Export)
-----------------------------------------------
stream rows → batch list     →  poll list (300ms)
map to MatchTransfer         →  flatten list to stream
queue.put(list) (blocks)     →  ExportService.exportMatches(stream)
                              →  ScheduleXProducer.sendMessage(payload)
```

---

## 10. External Contracts and Assumptions
- DomainService.getActiveDomains()
- MatchingGroupRepository.findGroupIdsByDomainId(domainId)
- ResponseMakerUtility.buildMatchTransfer(entity) → MatchTransfer
- ExportService.exportMatches(Supplier<Stream<MatchTransfer>>, groupId, domainId) → CompletableFuture<ExportedFile>
- GraphRequestFactory.buildFileReference(groupId, filePath, fileName, contentType, domainId) → MatchSuggestionsExchange
- ScheduleXProducer.sendMessage(topic, key, jsonPayload, sync=false)
- QueryUtils.getAllPotentialMatchesStreamingSQL(), getAllPerfectMatchesStreamingSQL()

---

## 11. Risks, Nuances, Recommendations
- Memory pressure
    - Queue capacity (100) × batchSize (100k) → potentially tens of millions of DTOs buffered. Recommend reducing batchSize and/or queue capacity, or draining more eagerly (smaller batches, or push-based export).
- Gauge registration
    - meterRegistry.gauge called per schedule run; ensure gauges are not re-registered repeatedly (prefer registering once with strong references).
- Completion semantics
    - done flag set when both producers complete; consumer continues until done && queue empty. Ensure producer exceptions are surfaced (CFs already propagate).
- Ordering/deduplication
    - Merged stream of potential + perfect; no dedupe or ordering guarantees. If client expects uniqueness or specific precedence, add merge/dedup logic.
- Backpressure observability
    - Consider publishing queue fill ratio for the internal transfer queue (not only executor queues).
- Fire-and-forget scheduling
    - Scheduler doesn’t await group CFs; ensure thread pool sizing accommodates peak parallelism, or add throttling at scheduler layer if needed.

---

## 12. Testing Strategy
- Unit
    - Processor: queue behavior, done/termination, error propagation
    - Streaming services: batch boundaries, retry on SQLException
    - Export + publish: topic/key formatting, payload serialization
- Integration
    - End-to-end for one group: DB → stream → export file → message produced
    - Dual streams simultaneously; simulate one failing and one succeeding
- Performance
    - Large datasets: tune batchSize and queue capacity; verify GC/memory stability
    - Throughput under multiple concurrent groups; measure match_process_duration and export_send_duration
- Resilience
    - Trigger circuit breaker; verify fallback behavior
    - Inject transient export errors; verify @Retryable works and metrics reflect retries

---

This LLD details the classes, concurrency, buffering, algorithms, failure handling, and operational aspects for the Match Transfer to Client module.