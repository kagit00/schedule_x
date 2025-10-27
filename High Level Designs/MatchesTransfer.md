# Match Transfer to Client — High‑Level Design (HLD)

## 1) Purpose and Scope
- Objective: Periodically export matches (potential and perfect) for each group/domain to a client-consumable artifact and notify downstream systems.
- Scope:
    - Batch/scheduled execution only.
    - Streams matches from DB in large pages.
    - Transforms DB entities to transfer DTOs.
    - Exports to a file/object via ExportService and publishes a file reference to a messaging topic (ScheduleXProducer).

---

## 2) Triggers and Entry Points
- Scheduler: MatchesTransferScheduler.scheduledMatchesTransferJob
    - Cron expression: ${match.transfer.cron-schedule}
    - Enumerates active domains and their groups.
    - Submits per‑group transfer tasks to a group-level executor.

---

## 3) Architecture Overview
- Orchestration
    - MatchesTransferScheduler: iterates domains/groups, dispatches async per-group transfers, captures per-group timing and failures, and exposes executor health gauges.
    - MatchTransferService: thin façade delegating to processor.
    - MatchTransferProcessor: coordinates streaming, batching, export, and messaging with resilience.

- Data ingestion
    - PotentialMatchStreamingService: JDBC forward-only streaming of PotentialMatchEntity in fetchSize batches, with automatic retry.
    - PerfectMatchStreamingService: same for PerfectMatchEntity.

- Transformation and export
    - ResponseMakerUtility.buildMatchTransfer: maps DB entity → MatchTransfer DTO.
    - ExportService.exportMatches: consumes a lazy Supplier<Stream<MatchTransfer>> and produces an ExportedFile (path, name, contentType).
    - ScheduleXProducer: publishes MatchSuggestionsExchange (file reference) to a topic per domain.

- Concurrency and buffering
    - Producer-consumer pipeline using a bounded LinkedBlockingQueue<List<MatchTransfer>> to bridge streaming producers and export consumer.
    - Two producer futures (potential + perfect) feed the shared queue; a single consumer stream drains and feeds ExportService.

---

## 4) End‑to‑End Flow
1) Scheduler loads active domains and groupIds.
2) For each group:
    - Submit async task to process the group (MatchTransferService → MatchTransferProcessor).
    - Processor:
        - Start two streaming producers (PotentialMatchStreamingService, PerfectMatchStreamingService).
        - For each DB batch: map to MatchTransfer list and enqueue into a bounded queue (backpressure).
        - Build a lazy Stream<MatchTransfer> that polls the queue and terminates when producers complete and the queue is empty.
        - exportAndSend:
            - Call ExportService.exportMatches(streamSupplier, groupId, domainId) → ExportedFile.
            - Build MatchSuggestionsExchange from file reference.
            - Publish to topic "<domain-name>-matches-suggestions" with key "<domainId>-<groupId>".
3) Scheduler collects per‑group metrics and logs; continues scanning remaining groups.

---

## 5) Components and Responsibilities

- MatchesTransferScheduler
    - Cron trigger; enumerates work; submits per-group tasks to matchTransferGroupExecutor.
    - Gauges for group executor: active threads, queue size (warn at high depth).
    - Per-group timer: group_process_duration; error counter group_process_failed.

- MatchTransferService
    - Thin wrapper to invoke MatchTransferProcessor.

- MatchTransferProcessor
    - Core pipeline with backpressure and resilience.
    - Starts two streaming producers (potential and perfect).
    - Bounded queue (capacity=100 lists) buffers MatchTransfer batches.
    - Lazy consumer stream built via queue.poll with time-bound waits; termination via done flag.
    - Export + notify:
        - ExportService.exportMatches(...).join() returning ExportedFile.
        - ScheduleXProducer.sendMessage(...) to domain-scoped topic.
    - Resilience:
        - @CircuitBreaker(name="matchProcessor", fallback → no-op with metrics).
        - @Retryable on export send (Connect/Timeout) with exponential backoff.
    - Metrics:
        - match_export_batch_count (type=potential/perfect)
        - match_export_records (total per group)
        - match_export_success
        - export_send_duration, match_process_duration
        - batch executor gauges and queue depth warnings

- PotentialMatchStreamingService / PerfectMatchStreamingService
    - JDBC streaming (forward-only, read-only, fetch-size = match.transfer.batch-size).
    - Retries up to 3 times on SQL exceptions (incremental backoff).
    - Validates row fields; batches delivered via Consumer<List<Entity>>.

- ExportService (external contract)
    - Accepts Supplier<Stream<MatchTransfer>>, produces ExportedFile with location metadata.
    - Streaming nature avoids loading entire dataset into memory.

- ScheduleXProducer (external contract)
    - Publishes JSON-serialized MatchSuggestionsExchange to a Kafka-like topic.

---

## 6) Data Flow
- Inputs
    - PotentialMatchEntity and PerfectMatchEntity from DB, streamed by groupId + domainId.
- Transform
    - Map each entity to MatchTransfer DTO (includes IDs and scores).
    - Merge two streams (potential + perfect) via a bounded queue.
- Output
    - ExportedFile (filePath, fileName, contentType).
    - MatchSuggestionsExchange payload with file reference published to topic.

---

## 7) Concurrency, Backpressure, and Memory
- Executors
    - matchTransferGroupExecutor: schedules per-group jobs across domains.
    - matchTransferExecutor: runs streaming producers and export consumer.
- Backpressure
    - Bounded queue of size 100 (each item is a List<MatchTransfer> of size batchSize).
    - Producers block on queue.put when full; consumer polls and streams to ExportService.
- Batch sizes
    - match.transfer.batch-size default 100,000 records per DB fetch → each list can be large; memory sizing must consider 100 × batchSize worst-case in queue.
- Termination
    - AtomicBoolean done set when both producers finish; consumer terminates when done and queue drained.

---

## 8) Resilience and Error Handling
- Circuit breaker
    - on processMatchTransfer; fallback logs and increments match_circuit_breaker_tripped.
- Retries
    - Streaming services retry up to 3 times (SQL).
    - exportAndSendSync retries ConnectException/Timeout up to 3 attempts (exponential backoff).
- Error metrics
    - group_process_failed, match_process_failed counters.
- Defensive checks
    - Null field filtering on streamed rows; non-blocking queue poll with timeouts; interruption handling with Thread.currentThread().interrupt() propagation.

---

## 9) Observability
- Gauges
    - group_executor_active_threads / group_executor_queue_size
    - batch_executor_active_threads / batch_executor_queue_size (warn at high depth)
- Timers
    - group_process_duration (per group)
    - match_process_duration (per group)
    - export_send_duration (per group)
- Counters
    - match_export_batch_count (type=potential/perfect)
    - match_export_records (total records processed)
    - match_export_success
    - group_process_failed, match_process_failed
    - queue depth warning counters (group and batch executors)

All metrics tagged at least with groupId (and sometimes threshold/type).

---

## 10) Configuration (Selected)
- match.transfer.cron-schedule: cron for scheduler.
- match.transfer.batch-size: DB fetch size per streamed batch (default 100000).
- Thread pools:
    - matchTransferGroupExecutor (ThreadPoolTaskExecutor): controls per-group parallelism.
    - matchTransferExecutor (ThreadPoolTaskExecutor): controls producer/consumer parallelism during export.
- Topic naming:
    - Topic = "<domain-name-lowercase>-matches-suggestions"
    - Key = "<domainId>-<groupId>"

---

## 11) Sequence (Per Group)

```
Scheduler
  → submit async: MatchTransferService.processGroup(groupId, domain)

MatchTransferProcessor.processMatchTransfer
  → start potentialFuture: streamAllMatches(potential) → batches → map → queue.put
  → start perfectFuture: streamAllMatches(perfect) → batches → map → queue.put
  → build matchesSupplier: Stream.generate(queue.poll(300ms))
       .takeWhile(!(done && (batch == null || empty)))
       .flatMap(list→stream)
  → exportAndSend(matchesSupplier)
     - ExportService.exportMatches(...).join() → ExportedFile
     - Build MatchSuggestionsExchange
     - Produce to topic via ScheduleXProducer
  → metrics: batch counts, total records, durations, success/fail counters
```

---

## 12) Security & Data Integrity
- Data integrity
    - Raw transfers reflect DB state; both potential and perfect matches are exported; no deduplication at this layer (downstream must handle if required).
- Security
    - Exported file path and reference are sent via message; ensure storage ACLs and topic authorization are appropriately configured.
    - Sensitive fields should be sanitized in ResponseMakerUtility mapping if applicable.

---

## 13) Risks & Considerations
- Memory pressure
    - Queue capacity of 100 × batchSize lists can be large (e.g., 100 × 100k DTOs). Ensure sizing of batchSize and queue capacity match heap limits; consider reducing batchSize or queue capacity, or exporting directly on streaming callbacks (push-based).
- Ordering and duplication
    - Potential + perfect streams are merged; no global dedupe or ordering guarantees. If clients require dedupe, consider key-based merging or flags.
- Backpressure visibility
    - Only queue depth warnings for executors are emitted; consider additional metrics for queue fill percentage for the MatchTransfer queue itself.
- Failure semantics
    - If one stream fails and the other completes, done flag is set only when bothComplete callback runs; ensure retries/logging provide sufficient signals.
- Producer/Consumer coupling
    - ExportService takes a Supplier<Stream<MatchTransfer>>. If export blocks too long, producers may stall on queue.put; ensure export throughput matches streaming rate.

---

## 14) Extensibility
- Add new export formats or transports by extending ExportService and the MatchSuggestionsExchange schema.
- Support additional match sources (e.g., post-processed matches) by adding new streaming services feeding the same queue.
- Enhance deduplication/partitioning within MatchTransferProcessor if client contracts evolve (e.g., separate topics per match type).

---

This HLD captures the scheduled match transfer module’s intent, flow, components, concurrency/backpressure, resilience, and observability.