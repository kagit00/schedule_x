```markdown
<div style="font-family: 'Consolas', 'Lato', monospace; font-size: 10pt; line-height: 1.8; max-width: 1200px; margin: 0 auto;">

<h1 style="font-weight: 200; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.5px;">Graph Matching Platform (ScheduleX)</h1>

<p style="margin-bottom: 1.5em;">
A high-throughput <strong>matching engine</strong> built for large-scale graph-based pairing — powering systems like dating, job, or ride-sharing platforms.<br/>
It processes millions of nodes, generates <strong>potential matches</strong> using <strong>Locality-Sensitive Hashing (LSH)</strong> and <strong>bipartite pairing</strong>, and finalizes <strong>perfect matches</strong> through algorithmic selection (Hungarian, Hopcroft–Karp, Auction, or Greedy).<br/>
Designed for <strong>scalability, resilience, and precision</strong>, the system emphasizes idempotent ingestion, adaptive concurrency, and fault-tolerant pipelines.
</p>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">At a Glance</h2>

<table style="width: 100%; border-collapse: collapse; margin-bottom: 2em;">
<thead>
<tr style="background-color: #f5f5f5;">
<th style="padding: 12px; text-align: left; border: 1px solid #ddd; font-weight: 400;">Feature</th>
<th style="padding: 12px; text-align: left; border: 1px solid #ddd; font-weight: 400;">Description</th>
</tr>
</thead>
<tbody>
<tr>
<td style="padding: 10px; border: 1px solid #ddd;"><strong>Architecture</strong></td>
<td style="padding: 10px; border: 1px solid #ddd;">Modular microservice pipeline — Ingestion → Matching → Distribution</td>
</tr>
<tr style="background-color: #fafafa;">
<td style="padding: 10px; border: 1px solid #ddd;"><strong>Tech Stack</strong></td>
<td style="padding: 10px; border: 1px solid #ddd;">Java, PostgreSQL, Kafka, MapDB, CopyManager, Docker</td>
</tr>
<tr>
<td style="padding: 10px; border: 1px solid #ddd;"><strong>Core Algorithms</strong></td>
<td style="padding: 10px; border: 1px solid #ddd;">LSH (Symmetric), Hopcroft–Karp, Hungarian, Auction, Greedy Top-K</td>
</tr>
<tr style="background-color: #fafafa;">
<td style="padding: 10px; border: 1px solid #ddd;"><strong>Performance Highlights</strong></td>
<td style="padding: 10px; border: 1px solid #ddd;">PostgreSQL COPY for bulk ingest, two-tier storage (in-memory + MapDB), bounded queues for backpressure</td>
</tr>
<tr>
<td style="padding: 10px; border: 1px solid #ddd;"><strong>Reliability</strong></td>
<td style="padding: 10px; border: 1px solid #ddd;">Idempotent writes, DLQ, retry/backoff, semaphore-based concurrency gating</td>
</tr>
<tr style="background-color: #fafafa;">
<td style="padding: 10px; border: 1px solid #ddd;"><strong>Use Cases</strong></td>
<td style="padding: 10px; border: 1px solid #ddd;">Dating (symmetric), Freelance/job (bipartite), Rider-driver, Mentorship</td>
</tr>
</tbody>
</table>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">My Role</h2>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">Architected the full matching pipeline (nodes import → candidate generation → final match export).</li>
<li style="margin-bottom: 0.8em;">Implemented <strong>high-throughput ingestion</strong> via PostgreSQL CopyManager.</li>
<li style="margin-bottom: 0.8em;">Designed <strong>two-tier storage strategy</strong> (MapDB staging + bulk COPY) to isolate DB load.</li>
<li style="margin-bottom: 0.8em;">Developed the <strong>PerfectMatchEngine</strong> with <strong>dynamic algorithm selection</strong> based on graph characteristics.</li>
<li style="margin-bottom: 0.8em;">Implemented concurrency control, DLQ handling, and backpressure across modules.</li>
</ul>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">Repo Structure</h2>

```
/src               → Service code (modules 1–4)
/docs/technical/   → Deep technical diagrams (Mermaid)
/config/           → Kafka + DB configs
README.md          → Overview (this file)
```

<p style="margin: 2em 0;">
For full internal flow and design reasoning, see the deep dive below 👇
</p>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h1 style="font-weight: 200; margin-top: 3em; margin-bottom: 1.5em; letter-spacing: 0.5px;">The Graph Matching Platform (Detailed Technical Overview)</h1>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">1. System Architecture: A Multi-Stage Pipeline</h2>

<p style="margin-bottom: 2em;">
The platform operates as a sequential pipeline where data is progressively refined at each stage. Each module is an independently deployable microservice or a distinct logical component within a monolith, designed with clear boundaries and contracts.
</p>

```mermaid
graph TD
    subgraph Ingestion [Module 1: Data Ingestion]
        A["Kafka Topics<br/>.*-users"] --> B[Nodes Import Module]
    end

    subgraph Persistence [Primary Datastore]
        C[(PostgreSQL Database)]
    end

    subgraph Computation [Module 2 & 3: The Matching Engine]
        D[Potential Matches Module]
        E[Perfect Matches Module]
    end

    subgraph Distribution [Module 4: Client Delivery]
        F[Match Transfer Module]
        G["Exported Files<br/>S3 or NFS"]
        H["Kafka Topics<br/>*-suggestions"]
    end

    %% Data Flow
    B -- "1. Idempotent Bulk Upsert<br/>(via Staged COPY Command)" --> C
    C -- "2. JDBC Streaming of Nodes" --> D
    D -- "3. Writes Candidate Matches<br/>(via Two-Tier Storage)" --> C
    C -- "4. JDBC Streaming of Candidates" --> E
    E -- "5. Writes Final Matches<br/>(Graph Algorithms)" --> C
    C -- "6. Dual-Producer Streaming" --> F
    F -- "7. Exports & Publishes Notification" --> G & H
```

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">2. Module 1: Nodes Import Module</h2>

<p style="margin-bottom: 1.5em;">
<strong>Core Responsibility</strong>: To consume node data from Kafka at scale and persist it idempotently into PostgreSQL, providing a reliable foundation for all downstream processing.
</p>

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">2.1. Architectural Deep Dive</h3>

<p style="margin-bottom: 2em;">
The module is architected around a staged, asynchronous processing model to decouple Kafka consumption from database I/O, allowing each to operate at its own optimal pace under a unified backpressure system.
</p>

```mermaid
graph TD
    A["Kafka Consumer Threads<br/>Concurrency: 4"] --> B[Payload Processor]
    B --> C[Import Orchestration Service]

    C -->|"1. Creates Job Record<br/>Status: PENDING"| F["DB: job_status Table"]
    C -->|"2. Routes to appropriate<br/>workflow (Cost vs Non-Cost)"| D{Batch Processing Engine}
    D -->|"3. Submits Batches to Executor"| G["nodesImportExecutor<br/>Queue Capacity: 100"]
    G --> E[Storage Layer]

    subgraph High_Throughput_Persistence ["High-Throughput Persistence"]
        E -->|"4. Builds In-Memory CSV"| H["PostgreSQL COPY Command"]
        H -->|"5. INSERT ... ON CONFLICT DO UPDATE"| I["DB: nodes Table"]
    end

    I -->|"Returns Upserted IDs"| E
    E -->|"Reports Progress"| D
    D -->|"6. Atomically Updates Job Stats"| F
```

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">2.2. Granular Discussion & Key Design Decisions</h3>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">High-Throughput Persistence: The <code>COPY</code> Command</h4>

<p style="margin-bottom: 1.5em;">
Instead of using standard JDBC batch inserts, this module leverages PostgreSQL's <code>COPY</code> command via the <code>CopyManager</code> API. This decision was driven by performance benchmarks:
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;"><strong>Standard JDBC Batch Insert</strong>: ~5,000 rows/second</li>
<li style="margin-bottom: 0.8em;"><strong>COPY Command</strong>: ~50,000 rows/second (10x improvement)</li>
</ul>

<p style="margin-bottom: 1.5em;"><strong>How It Works</strong>:</p>

<ol style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">Data is buffered in memory as a CSV-formatted <code>StringWriter</code>.</li>
<li style="margin-bottom: 0.8em;">When the buffer reaches a threshold (e.g., 10,000 rows), it's flushed to a temporary staging table via <code>COPY temp_staging FROM STDIN WITH (FORMAT CSV)</code>.</li>
<li style="margin-bottom: 0.8em;">A single SQL statement then performs: <code>INSERT INTO nodes SELECT * FROM temp_staging ON CONFLICT (external_id) DO UPDATE SET ...</code></li>
<li style="margin-bottom: 0.8em;">The staging table is truncated for the next batch.</li>
</ol>

<p style="margin-bottom: 2em;">
<strong>Rationale</strong>: This approach combines the speed of <code>COPY</code> with the idempotency of <code>ON CONFLICT</code>. It's the fastest way to achieve upsert semantics in PostgreSQL at scale.
</p>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Backpressure: The Bounded Executor Queue</h4>

<p style="margin-bottom: 1.5em;">
The <code>nodesImportExecutor</code> is configured with a <code>LinkedBlockingQueue</code> of capacity 100. When the queue is full:
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">The submitting thread (Kafka consumer) will block on <code>executor.submit(task)</code>.</li>
<li style="margin-bottom: 0.8em;">Kafka's <code>max.poll.interval.ms</code> is set to 5 minutes, giving ample time for the queue to drain.</li>
<li style="margin-bottom: 0.8em;">If the queue doesn't drain in time, the consumer group will rebalance, and the partition will be reassigned to another consumer.</li>
</ul>

<p style="margin-bottom: 2em;">
<strong>Rationale</strong>: This design ensures that the application never consumes more data from Kafka than it can process, preventing memory exhaustion. The bounded queue acts as a pressure valve, naturally throttling the ingest rate to match the database's write capacity.
</p>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Idempotency: Handling Duplicate Messages</h4>

<p style="margin-bottom: 1.5em;">
Kafka does not guarantee exactly-once delivery in all failure scenarios. To ensure data integrity:
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">Each node has a unique <code>external_id</code> (e.g., <code>user_12345</code>).</li>
<li style="margin-bottom: 0.8em;">The <code>nodes</code> table has a unique constraint on <code>external_id</code>.</li>
<li style="margin-bottom: 0.8em;">The upsert logic is: <code>ON CONFLICT (external_id) DO UPDATE SET updated_at = NOW(), ...</code></li>
</ul>

<p style="margin-bottom: 2em;">
<strong>Result</strong>: If the same Kafka message is processed twice (e.g., due to a rebalance), the second processing will simply update the existing row with the same data, resulting in a no-op. This makes the entire ingestion pipeline idempotent.
</p>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">3. Module 2: Potential Matches Module</h2>

<p style="margin-bottom: 1.5em;">
<strong>Core Responsibility</strong>: To generate a list of candidate matches for each node, filtering the search space from millions to thousands using efficient approximation techniques.
</p>

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">3.1. Architectural Deep Dive</h3>

<p style="margin-bottom: 2em;">
This module is the computational workhorse of the platform. It must process millions of nodes and generate billions of potential edges, all while staying within memory and time constraints.
</p>

```mermaid
graph TD
    A["Scheduler<br/>Cron: Every 5 mins"] --> B[Potential Match Orchestrator]
    B -->|"Fetches Active Groups"| C["DB: groups Table"]
    C -->|"Returns Group IDs"| B
    B -->|"For Each Group"| D[Concurrency Gate]

    D -->|"Semaphore: Max 2 Concurrent Groups"| E[Match Generation Service]

    E -->|"Streams Nodes via JDBC"| F["DB: nodes Table<br/>Batch Size: 5,000"]
    F -->|"In-Memory Accumulation"| G[LSH Index Builder]

    subgraph Matching_Engine ["Matching Engine"]
        G -->|"Builds Hash Tables"| H["LSH Index<br/>(MinHash + Banding)"]
        H -->|"For Each Node"| I[Candidate Retrieval]
        I -->|"Fetches Buckets"| H
        I -->|"Applies Filters<br/>(Age, Location, etc.)"| J[Similarity Scorer]
        J -->|"Computes Jaccard Distance"| K[Top-K Selector]
    end

    K -->|"Generates Match Edges"| L[Two-Tier Storage Layer]

    subgraph Persistence_Strategy ["Two-Tier Persistence"]
        L -->|"Stage 1: Immediate Write"| M["MapDB<br/>(Memory-Mapped File)"]
        L -->|"Stage 2: Async Flush (Every 10,000)"| N["PostgreSQL<br/>via COPY Command"]
    end

    N -->|"Final Insert"| O["DB: potential_matches Table"]
```

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">3.2. Granular Discussion & Key Design Decisions</h3>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Why LSH? (Locality-Sensitive Hashing)</h4>

<p style="margin-bottom: 1.5em;">
A naive approach to matching would compute the similarity between every pair of nodes: O(n²). For 1 million nodes, that's 1 trillion comparisons — computationally infeasible.
</p>

<p style="margin-bottom: 1.5em;">
<strong>LSH reduces this to O(n)</strong> by using a probabilistic data structure:
</p>

<ol style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;"><strong>MinHash</strong>: Each node's feature set (interests, skills, etc.) is hashed into a fixed-size signature (e.g., 128 integers).</li>
<li style="margin-bottom: 0.8em;"><strong>Banding</strong>: The signature is split into bands (e.g., 16 bands of 8 hashes each). Each band is hashed into a bucket.</li>
<li style="margin-bottom: 0.8em;"><strong>Collision = Candidate</strong>: If two nodes hash to the same bucket in any band, they're considered candidates.</li>
</ol>

<p style="margin-bottom: 2em;">
<strong>Guarantees</strong>: LSH is tuned such that:
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">Nodes with >70% similarity have a 90% chance of being retrieved.</li>
<li style="margin-bottom: 0.8em;">Nodes with <30% similarity have a <5% chance of being retrieved.</li>
</ul>

<p style="margin-bottom: 2em;">
This dramatically reduces the search space while maintaining high recall for relevant matches.
</p>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Two-Tier Storage: Decoupling Speed from Durability</h4>

<p style="margin-bottom: 1.5em;">
Writing matches directly to PostgreSQL during generation would create a bottleneck:
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">Each INSERT requires a network round-trip + disk I/O.</li>
<li style="margin-bottom: 0.8em;">This would slow down the matching loop, causing the in-memory LSH index to grow unbounded.</li>
</ul>

<p style="margin-bottom: 1.5em;"><strong>Solution: Staged Writes</strong></p>

<ol style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;"><strong>Tier 1 (Fast)</strong>: Matches are immediately written to a <code>MapDB</code> instance, which is a memory-mapped file. This is an append-only operation, taking ~1 microsecond.</li>
<li style="margin-bottom: 0.8em;"><strong>Tier 2 (Durable)</strong>: Every 10,000 matches, a background thread reads from MapDB, formats the data as CSV, and flushes it to PostgreSQL via <code>COPY</code>.</li>
</ol>

<p style="margin-bottom: 2em;">
<strong>Benefits</strong>:
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">The matching loop runs at in-memory speeds.</li>
<li style="margin-bottom: 0.8em;">Database writes are batched and optimized.</li>
<li style="margin-bottom: 0.8em;">If the process crashes, MapDB can be replayed (it's crash-safe).</li>
</ul>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Concurrency Control: The Semaphore Pattern</h4>

<p style="margin-bottom: 1.5em;">
Processing multiple groups concurrently improves throughput, but each group's processing is memory-intensive (building an LSH index for millions of nodes).
</p>

<p style="margin-bottom: 1.5em;">
<strong>Mechanism</strong>: A <code>Semaphore(2)</code> limits the number of concurrent group-processing jobs to 2.
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;">When a new group is scheduled, the orchestrator calls <code>semaphore.tryAcquire()</code>.</li>
<li style="margin-bottom: 0.8em;">If the semaphore is exhausted (2 groups already processing), the new group waits in a queue.</li>
<li style="margin-bottom: 0.8em;">When a group finishes, it calls <code>semaphore.release()</code>, allowing the next group to start.</li>
</ul>

<p style="margin-bottom: 2em;">
<strong>Rationale</strong>: This prevents memory thrashing. On a machine with 16GB RAM, processing 3+ groups simultaneously would cause excessive GC pauses or OOM errors. By limiting to 2, we ensure each job has ~6-7GB of working memory.
</p>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">4. Module 3: Perfect Matches Module</h2>

<p style="margin-bottom: 1.5em;">
<strong>Core Responsibility</strong>: To refine the list of potential matches into a final set of perfect matches using graph-theoretic algorithms, optimizing for global match quality.
</p>

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">3.3. Architectural Deep Dive</h3>

<p style="margin-bottom: 2em;">
This module treats the match problem as a graph optimization task. The choice of algorithm depends on the structure of the graph and the business requirements.
</p>

```mermaid
graph TD
    A["Scheduler<br/>Cron: Every 10 mins"] --> B[Perfect Match Orchestrator]
    B -->|"Fetches Groups with Pending Matches"| C["DB: potential_matches Table"]
    C -->|"Returns Group IDs + Edge Counts"| B

    B --> D[Strategy Selector]
    D -->|"Analyzes Graph Characteristics"| E{Decision Tree}

    E -->|"Bipartite + Small<br/>(n < 5,000)"| F["Hungarian Algorithm<br/>O(n³)"]
    E -->|"Bipartite + Large<br/>(n > 5,000)"| G["Auction Algorithm<br/>O(n² log n)"]
    E -->|"Symmetric + Sparse"| H["Hopcroft-Karp<br/>O(E√V)"]
    E -->|"Symmetric + Dense"| I["Greedy Top-K<br/>O(n log k)"]

    F --> J[Match Finalizer]
    G --> J
    H --> J
    I --> J

    J -->|"Validates Constraints"| K[Constraint Checker]
    K -->|"Ensures Mutual Consent, Max Matches, etc."| J
    J -->|"Bulk Insert via COPY"| L["DB: perfect_matches Table"]
```

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">3.4. Granular Discussion & Key Design Decisions</h3>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Dynamic Algorithm Selection</h4>

<p style="margin-bottom: 1.5em;">
There is no single "best" algorithm for graph matching; the optimal choice depends on the graph's structure and the desired outcome. This module codifies that domain knowledge.
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;"><strong>Rationale</strong>: By creating a <code>MatchingStrategySelector</code>, we make the system extensible and intelligent. Adding a new algorithm is as simple as implementing the <code>MatchingStrategy</code> interface and adding a rule to the selector. This avoids a one-size-fits-all approach that would be inefficient or incorrect for certain use cases.</li>
<li style="margin-bottom: 0.8em;"><strong>Example</strong>: Using the <code>Hungarian</code> algorithm on a 100,000-node graph would run for days. The selector correctly routes this to the <code>Auction</code> or <code>Hopcroft-Karp</code> algorithm, ensuring the job completes in a reasonable timeframe.</li>
</ul>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Memory-Aware Processing</h4>

<p style="margin-bottom: 1.5em;">
Executing graph algorithms on large datasets is extremely memory-intensive.
</p>

<ul style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;"><strong>Mechanism</strong>: The service is configured with a maximum memory budget (e.g., <code>1024MB</code>). Before processing a large batch, it checks <code>Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()</code>.</li>
<li style="margin-bottom: 0.8em;"><strong>Action</strong>: If current usage exceeds a threshold (e.g., 80% of the budget), the service will either:
  <ol style="margin-top: 0.5em; line-height: 1.8;">
    <li>Reduce the size of the next processing sub-batch.</li>
    <li>Gracefully fail the job with a clear "Memory limit exceeded" error.</li>
  </ol>
</li>
<li style="margin-bottom: 0.8em;"><strong>Rationale</strong>: This proactive monitoring prevents the JVM from throwing an <code>OutOfMemoryError</code>, which is an unrecoverable state. It ensures the service fails cleanly and predictably.</li>
</ul>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<h2 style="font-weight: 200; margin-top: 2.5em; margin-bottom: 1.2em; letter-spacing: 0.5px;">5. Module 4: Match Transfer to Client</h2>

<p style="margin-bottom: 1.5em;">
<strong>Core Responsibility</strong>: To reliably export the full set of matches (both potential and perfect) and notify the client.
</p>

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">4.1. Architecture: The Producer-Consumer Pattern</h3>

<p style="margin-bottom: 2em;">
This module is a textbook implementation of the Producer-Consumer pattern, designed for high I/O throughput and decoupled processing.
</p>

```mermaid
graph TB
    A["MatchTransferProcessor"] -->|"Starts Job for a Group"| X

    subgraph Producer_Threads ["Producer Threads (2 per Group)"]
        X -->|"Streams from potential_matches table"| P1["PotentialMatchStreamingService"]
        X -->|"Streams from perfect_matches table"| P2["PerfectMatchStreamingService"]
    end

    subgraph InMemory_Buffer ["Bounded In-Memory Buffer"]
        B["LinkedBlockingQueue<br/>Capacity: 100 Batches<br/>Acts as a shock absorber"]
    end

    subgraph Consumer_Thread ["Consumer Thread (1 per Group)"]
        C["Export & Publish Service"]
    end

    P1 -->|"queue.put(batch)<br/>Blocks if full"| B
    P2 -->|"queue.put(batch)<br/>Blocks if full"| B
    C -->|"queue.poll(300ms)<br/>Waits if empty"| B

    subgraph Output_Sinks ["Output Sinks"]
        D["ExportService<br/>Writes to File"]
        E["ScheduleXProducer<br/>Sends Kafka Notification"]
    end

    C -->|"Lazily consumes stream"| D
    C -->|"After file is written"| E
```

<h3 style="font-weight: 300; margin-top: 2em; margin-bottom: 1em; letter-spacing: 0.3px;">4.2. Granular Discussion & Key Design Decisions</h3>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Why Producer-Consumer?</h4>

<ol style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;"><strong>Decoupling</strong>: It separates the concern of <em>reading</em> data from the database from the concern of <em>writing</em> data to a file/Kafka. The database streaming can run at full speed while the file I/O or Kafka producer handles its own latency.</li>
<li style="margin-bottom: 0.8em;"><strong>Parallelism</strong>: It allows I/O operations (reading from DB, writing to file) to happen concurrently, maximizing throughput.</li>
<li style="margin-bottom: 0.8em;"><strong>Backpressure</strong>: The <code>LinkedBlockingQueue</code> is the key. If the consumer (file writing) is slow, the queue fills up, and the producers (DB readers) will naturally block. This prevents the application from reading an unbounded amount of data from the database into memory.</li>
</ol>

<h4 style="font-weight: 300; margin-top: 1.5em; margin-bottom: 0.8em;">Termination Logic: A Classic Concurrency Problem</h4>

<p style="margin-bottom: 1.5em;">
Ensuring the consumer shuts down correctly without losing data is non-trivial.
</p>

<ol style="margin-bottom: 2em; line-height: 2;">
<li style="margin-bottom: 0.8em;"><strong>Producer Completion</strong>: Both producer tasks are wrapped in <code>CompletableFuture</code>s.</li>
<li style="margin-bottom: 0.8em;"><strong><code>CompletableFuture.allOf(...)</code></strong>: The main thread waits for both producers to finish.</li>
<li style="margin-bottom: 0.8em;"><strong><code>done</code> Flag</strong>: An <code>AtomicBoolean done</code> flag is set to <code>true</code> once <code>allOf</code> completes.</li>
<li style="margin-bottom: 0.8em;"><strong>Consumer Loop Condition</strong>: The consumer's loop is <code>while (!done || !queue.isEmpty())</code>. This elegant condition means: "Keep running as long as the producers are not done, OR as long as there is still data in the queue to process."</li>
<li style="margin-bottom: 0.8em;"><strong>Result</strong>: This guarantees that the consumer will process every last item placed in the queue before shutting down, ensuring zero data loss.</li>
</ol>

<hr style="margin: 3em 0; border: none; border-top: 1px solid #ddd;"/>

<div style="margin-top: 4em; padding: 2em; background-color: #f9f9f9; border-left: 4px solid #0066cc;">
<p style="margin: 0; font-style: italic;">
This architecture represents a production-grade system designed for scale, reliability, and maintainability. Every design decision is a deliberate trade-off between competing concerns: speed vs. durability, simplicity vs. flexibility, memory vs. throughput.
</p>
</div>

</div>
```