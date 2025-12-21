# Potential Matches Creation System - High-Level Design Document


---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#3-system-architecture)
3. [Functional Architecture](#4-functional-architecture)
4. [Non-Functional Requirements](#5-non-functional-requirements)
5. [Technology Stack](#6-technology-stack)
6. [Data Architecture](#7-data-architecture)
7. [Integration Architecture](#8-integration-architecture)
8. [Scalability & Performance](#11-scalability--performance)


---

## 1. Executive Summary

### 1.1 System Overview

The **Potential Matches Creation System** is an enterprise-grade graph processing platform designed to compute and persist compatibility relationships between entities at scale. The system processes millions of nodes daily, generating match recommendations using sophisticated algorithms including LSH (Locality-Sensitive Hashing), metadata-based weighting, and flat comparison strategies.

### 1.2 Business Value Proposition

```mermaid
flowchart TB
    ROOT["Business Value"]

    ROOT --> AUTO["Automation"]
    ROOT --> SC["Scale"]
    ROOT --> QL["Quality"]
    ROOT --> EF["Efficiency"]
    ROOT --> INT["Intelligence"]

    AUTO --> A1["Eliminates manual matching"]
    AUTO --> A2["Reduces human error"]
    AUTO --> A3["24/7 processing capability"]

    SC --> S1["Millions of entities"]
    SC --> S2["Billions of comparisons"]
    SC --> S3["Sub-linear complexity"]

    QL --> Q1["Algorithmic consistency"]
    QL --> Q2["Configurable strategies"]
    QL --> Q3["Continuous improvement"]

    EF --> E1["Cost reduction vs manual"]
    EF --> E2["Fast processing time"]
    EF --> E3["Resource optimization"]

    INT --> I1["LSH similarity detection"]
    INT --> I2["Metadata weighting"]
    INT --> I3["Adaptive learning"]

    style ROOT fill:#ECEFF1
    style AUTO fill:#E3F2FD
    style SC fill:#E8F5E9
    style QL fill:#FFFDE7
    style EF fill:#FCE4EC
    style INT fill:#EDE7F6

```

### 1.3 Key Metrics

| Metric | Current Capacity | Target (12 months) | Strategic Goal (24 months) |
|--------|------------------|-------------------|---------------------------|
| **Nodes Processed/Day** | 50K | 500K | 5M |
| **Match Computations/Sec** | 100K | 500K | 2M |
| **Processing Latency** | 15 min/group | <10 min/group | <5 min/group |
| **System Availability** | 99.5% | 99.9% | 99.99% |
| **Concurrent Domains** | 2 | 5 | 20 |
| **Storage Footprint** | 200GB | 2TB | 20TB |

### 1.4 Strategic Alignment

```mermaid
graph LR
    A[Business Strategy:<br/>AI-Powered Matching] --> B[Platform Capability:<br/>Graph Processing]
    B --> C[System Feature:<br/>Potential Matches]
    C --> D[Business Outcome:<br/>Revenue Growth]
    
    E[Market Demand:<br/>Personalization] --> F[Technical Innovation:<br/>LSH + Metadata]
    F --> C
    
    G[Operational Excellence:<br/>Automation] --> H[System Design:<br/>Scheduled Batch]
    H --> C
    
    style A fill:#4CAF50
    style B fill:#2196F3
    style C fill:#FF9800
    style D fill:#9C27B0
```

---


## 3. System Architecture

### 3.1 Architectural Style

**Primary Style:** Event-Driven Batch Processing  
**Secondary Patterns:** Microkernel, Pipes-and-Filters

```mermaid
C4Context
    title System Context Diagram - Potential Matches Creation System
    
    Person(bizuser, "Business Applications", "Consumes match results via API/DB")
    Person(analyst, "Data Analysts", "Query match statistics")
    Person(ops, "Operations Team", "Monitor system health")
    
    System_Boundary(pms_boundary, "Potential Matches System") {
        System(pms, "Match Creation Engine", "Computes entity compatibility at scale")
    }
    
    System_Ext(nodeingest, "Node Ingestion Service", "Adds entities to system")
    System_Ext(scheduler, "Spring Scheduler", "Triggers daily processing")
    SystemDb_Ext(postgres, "PostgreSQL Cluster", "Master data & results store")
    SystemDb_Ext(lmdb, "LMDB Storage", "High-performance edge cache")
    System_Ext(monitoring, "Observability Stack", "Prometheus + Grafana + ELK")
    System_Ext(config, "Configuration Service", "Centralized config management")
    
    Rel(scheduler, pms, "Triggers", "Cron")
    Rel(nodeingest, postgres, "Writes nodes", "JDBC")
    Rel(pms, postgres, "Reads nodes, Writes matches", "JDBC")
    Rel(pms, lmdb, "Reads/Writes edges", "Memory-mapped")
    Rel(pms, monitoring, "Exports metrics & logs", "HTTP")
    Rel(pms, config, "Fetches config", "HTTP")
    Rel(bizuser, postgres, "Queries matches", "SQL")
    Rel(analyst, postgres, "Analytics queries", "SQL")
    Rel(ops, monitoring, "Views dashboards", "HTTPS")
    
    UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="2")
```

### 3.2 Logical Architecture

```mermaid
graph TB
    subgraph "Presentation Layer"
        A1[Scheduled Triggers<br/>@Scheduled Cron]
        A2[Manual API<br/>Future: REST Endpoints]
        A3[Monitoring Endpoints<br/>Actuator]
    end
    
    subgraph "Application Services Layer"
        B1[Orchestration Service<br/>Job Scheduling & Coordination]
        B2[Processing Service<br/>Business Logic]
        B3[Node Management Service<br/>Entity Lifecycle]
    end
    
    subgraph "Domain Layer"
        C1[Graph Processing<br/>Algorithms & Strategies]
        C2[Match Computation<br/>Scoring & Ranking]
        C3[Queue Management<br/>Buffering & Flow Control]
    end
    
    subgraph "Infrastructure Layer"
        D1[Data Access<br/>Repositories & DAOs]
        D2[Storage Abstraction<br/>LMDB + PostgreSQL]
        D3[Concurrency Control<br/>Semaphores & Thread Pools]
        D4[Observability<br/>Metrics, Logs, Traces]
    end
    
    subgraph "External Systems"
        E1[(PostgreSQL<br/>Master Database)]
        E2[(LMDB<br/>Edge Cache)]
        E3[Prometheus<br/>Metrics Store]
        E4[Configuration Server]
    end
    
    A1 --> B1
    A2 --> B2
    A3 --> D4
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
    
    C1 --> D1
    C2 --> D2
    C3 --> D3
    
    D1 --> E1
    D2 --> E2
    D4 --> E3
    B1 --> E4
    
    style A1 fill:#4CAF50
    style B1 fill:#2196F3
    style C1 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#607D8B
```

### 3.3 Component Architecture

```mermaid
C4Container
    title Container Diagram - Match Creation System
    
    Container_Boundary(app, "Application Container") {
        Container(scheduler, "Scheduler", "Spring @Scheduled", "Triggers batch jobs")
        Container(orchestrator, "Orchestration Service", "Java", "Manages workflow & concurrency")
        Container(processor, "Processing Service", "Java", "Node fetching & batching")
        Container(graphengine, "Graph Engine", "Java", "LSH, Metadata, Flat strategies")
        Container(queuemgr, "Queue Manager", "Java", "Memory + Disk buffering")
        Container(persistence, "Persistence Layer", "Java", "Dual storage writes")
    }
    
    ContainerDb(postgres, "PostgreSQL", "Relational DB", "Nodes & Matches")
    ContainerDb(lmdb, "LMDB", "KV Store", "Edge Cache")
    Container(monitoring, "Monitoring", "Prometheus/Grafana", "Metrics & Dashboards")
    
    Rel(scheduler, orchestrator, "Triggers", "In-process")
    Rel(orchestrator, processor, "Coordinates", "Async")
    Rel(processor, postgres, "Reads nodes", "JDBC")
    Rel(processor, graphengine, "Submits batches", "Async")
    Rel(graphengine, queuemgr, "Enqueues matches", "In-process")
    Rel(queuemgr, persistence, "Flushes batches", "Async")
    Rel(persistence, postgres, "Writes matches", "COPY Protocol")
    Rel(persistence, lmdb, "Writes edges", "Memory-mapped")
    Rel(orchestrator, monitoring, "Exports metrics", "HTTP")
    
    UpdateLayoutConfig($c4ShapeInRow="3")
```

---

## 4. Functional Architecture

### 4.1 Core Capabilities

```mermaid
graph TB
    subgraph "Job Orchestration"
        F1[Scheduled Execution]
        F2[Domain Partitioning]
        F3[Group Serialization]
        F4[Semaphore Control]
    end
    
    subgraph "Node Management"
        F5[Cursor-based Pagination]
        F6[Metadata Hydration]
        F7[Processed Flag Tracking]
        F8[Batch Fetching]
    end
    
    subgraph "Graph Processing"
        F9[Match Type Detection<br/>Symmetric/Bipartite]
        F10[Strategy Selection<br/>LSH/Metadata/Flat]
        F11[Parallel Edge Computation]
        F12[Top-K Filtering]
    end
    
    subgraph "Match Persistence"
        F13[Dual Storage<br/>LMDB + PostgreSQL]
        F14[Queue Buffering]
        F15[Disk Spillover]
        F16[Final Merging]
    end
    
    subgraph "Operational"
        F17[Error Recovery]
        F18[Cursor Resumption]
        F19[Metrics Collection]
        F20[Graceful Shutdown]
    end
    
    F1 --> F5
    F5 --> F9
    F9 --> F10
    F10 --> F11
    F11 --> F13
    F13 --> F14
    
    F2 --> F4
    F6 --> F8
    F12 --> F14
    F14 --> F15
    F15 --> F16
    
    F3 --> F17
    F7 --> F18
    F19 -.-> F1
    
    style F1 fill:#C8E6C9
    style F9 fill:#BBDEFB
    style F13 fill:#FFCCBC
    style F17 fill:#FFF9C4
```

### 4.2 Processing Pipeline

```mermaid
flowchart LR
    A[Scheduled<br/>Trigger] --> B[Get Active<br/>Domains]
    B --> C[For Each<br/>Domain]
    C --> D[Get Groups<br/>per Domain]
    D --> E{Acquire<br/>Domain Sem?}
    
    E -->|Timeout| F[Skip & Log]
    E -->|Success| G[Load Cursor<br/>Position]
    
    G --> H[Fetch Node Batch<br/>1000 IDs]
    H --> I{Nodes Found?}
    
    I -->|No| J[Empty Streak++]
    J --> K{Streak >= 3?}
    K -->|Yes| L[End Group]
    K -->|No| H
    
    I -->|Yes| M[Hydrate Metadata]
    M --> N[Build Graph]
    N --> O[Compute Edges]
    O --> P[Enqueue Matches]
    
    P --> Q{Queue Full?}
    Q -->|Yes| R[Spill to Disk]
    Q -->|No| S[Keep in Memory]
    
    R --> T[Flush Batch]
    S --> T
    
    T --> U[Save to LMDB]
    T --> V[Save to PostgreSQL]
    
    U --> W[Mark Processed]
    V --> W
    W --> X[Update Cursor]
    X --> H
    
    L --> Y[Drain Queue]
    Y --> Z[Final Save<br/>LMDB → SQL]
    Z --> AA[Release Semaphore]
    AA --> AB[Next Group]
    
    style A fill:#4CAF50
    style N fill:#2196F3
    style P fill:#FF9800
    style T fill:#9C27B0
    style Z fill:#F44336
```

### 4.3 Algorithm Selection Logic

```mermaid
flowchart TD
    A[Node Batch Ready] --> B[Query Node Metadata Keys]
    B --> C{Valid Keys Found?}
    
    C -->|No| D[Select Flat Strategy<br/>Simple pairwise comparison]
    C -->|Yes| E[Generate Weight Key<br/>groupId + sorted keys]
    
    E --> F{Strategy<br/>Registered?}
    F -->|No| G[Create ConfigurableMetadataWeightFunction]
    F -->|Yes| H[Retrieve from Registry]
    
    G --> I{Node Count<br/>> 1000?}
    H --> I
    
    I -->|Yes| J[Use LSH Strategy<br/>Sub-linear complexity]
    I -->|No| K[Use Metadata Strategy<br/>Full comparison]
    
    D --> L[Index Nodes<br/>Simple list]
    J --> M[Index Nodes<br/>Build LSH buckets]
    K --> N[Index Nodes<br/>Metadata encoding]
    
    L --> O[Process Batch<br/>O n^2 comparison]
    M --> P[Process Batch<br/>O n log n via LSH]
    N --> Q[Process Batch<br/>O n^2 weighted]
    
    O --> R[Generate Matches]
    P --> R
    Q --> R
    
    style D fill:#FFCDD2
    style J fill:#C8E6C9
    style K fill:#BBDEFB
    style R fill:#FFF9C4
```

---

## 5. Non-Functional Requirements

### 5.1 Performance Requirements

```mermaid
graph LR
    subgraph "Throughput"
        T1[Nodes: 1K/sec]
        T2[Edges: 100K/sec]
        T3[Matches Saved: 50K/sec]
    end
    
    subgraph "Latency"
        L1[Node Fetch: <2s per 1K]
        L2[Graph Build: <30s per 2K nodes]
        L3[DB Save: <5s per 50K matches]
    end
    
    subgraph "Scalability"
        S1[Nodes: 50M total]
        S2[Groups: 500 concurrent]
        S3[Domains: 20 concurrent]
    end
    
    subgraph "Efficiency"
        E1[Memory: <16GB heap]
        E2[CPU: <80% avg]
        E3[Storage: <10TB total]
    end
    
    style T1 fill:#C8E6C9
    style L1 fill:#BBDEFB
    style S1 fill:#FFF9C4
    style E1 fill:#FFCCBC
```

**Performance SLAs**:

| Metric | Target | Measurement | Tolerance |
|--------|--------|-------------|-----------|
| **Job Completion Time** | <15 min per group | End-to-end timer | ±20% |
| **Node Processing Rate** | ≥1000 nodes/sec | Counter/duration | ±15% |
| **Edge Computation Rate** | ≥100K edges/sec | Counter/duration | ±20% |
| **Database Write Rate** | ≥50K inserts/sec | PostgreSQL COPY | ±25% |
| **LMDB Read Latency** | <1ms p95 | Histogram | <5ms p99 |
| **Memory Utilization** | <80% heap | JVM metrics | <90% max |
| **CPU Utilization** | <70% avg | System metrics | <85% max |

### 5.2 Reliability Requirements

**Availability Target:** 99.5% monthly uptime (excluding planned maintenance)

**Failure Tolerance:**

```mermaid
graph TB
    A[Failure Scenario] --> B{Type}
    
    B -->|Transient| C[Automatic Retry<br/>Exponential Backoff]
    B -->|Permanent| D[Skip & Log<br/>Continue with Next]
    B -->|Systemic| E[Circuit Breaker<br/>Fast Fail]
    
    C --> F{Retry Count}
    F -->|<3| C
    F -->|≥3| G[Mark Failed<br/>Alert Operations]
    
    D --> H[Record Error Metrics]
    E --> I[Fallback Mode]
    
    G --> J[Daily Error Report]
    H --> J
    I --> J
    
    style C fill:#C8E6C9
    style D fill:#FFF9C4
    style E fill:#FFCCBC
    style G fill:#FFCDD2
```

**Data Integrity:**
- ACID transactions for all database writes
- Cursor-based resumability (no data loss on failure)
- Idempotent processing (safe to rerun)
- Advisory locks prevent concurrent updates

### 5.3 Scalability Requirements

```mermaid
graph LR
    subgraph "Current Scale"
        C1[2.5K nodes/group<br/>50 groups<br/>2 domains]
    end
    
    subgraph "12-Month Target"
        T1[10K nodes/group<br/>200 groups<br/>5 domains]
    end
    
    subgraph "24-Month Target"
        T2[50K nodes/group<br/>500 groups<br/>20 domains]
    end
    
    C1 -->|4x growth| T1
    T1 -->|5x growth| T2
    
    style C1 fill:#E3F2FD
    style T1 fill:#FFF9C4
    style T2 fill:#C8E6C9
```

**Scalability Strategies**:

| Dimension | Current Approach | Future Enhancement |
|-----------|------------------|-------------------|
| **Vertical** | 16GB RAM, 8 CPUs | 32GB RAM, 16 CPUs |
| **Horizontal** | Active-Passive | Domain-based Partitioning |
| **Storage** | Single PostgreSQL | Read replicas + Sharding |
| **Compute** | Thread pools | Distributed workers (Kafka) |
| **Caching** | LMDB local | Distributed cache (Redis) |

### 5.4 Maintainability Requirements

- **Code Coverage:** ≥80% unit tests, ≥60% integration tests
- **Documentation:** Inline JavaDoc, README per module
- **Logging:** Structured JSON logs, correlation IDs
- **Observability:** Prometheus metrics, Grafana dashboards
- **Deployment:** Blue-green deployments, rollback capability
- **Configuration:** Externalized via Spring Cloud Config

---

## 6. Technology Stack

### 6.1 Technology Landscape

```mermaid
graph TB
    subgraph "Application Tier"
        A1[Java 17 LTS]
        A2[Spring Boot 3.2.x]
        A3[Spring Framework 6.x]
        A4[Spring Batch Patterns]
    end
    
    subgraph "Data Tier"
        B1[PostgreSQL 15]
        B2[LMDB 0.9.x]
        B3[HikariCP 5.x]
        B4[Flyway Migration]
    end
    
    subgraph "Resilience Tier"
        C1[Resilience4j]
        C2[Spring Retry]
        C3[Semaphore Control]
        C4[Exponential Backoff]
    end
    
    subgraph "Observability Tier"
        D1[Micrometer]
        D2[Prometheus]
        D3[Grafana]
        D4[Logback + ELK]
    end
    
    subgraph "Build & Deploy"
        E1[Maven 3.9]
        E2[Docker]
        E3[Kubernetes Future]
        E4[GitHub Actions]
    end
    
    A1 --> B1
    A2 --> C1
    A3 --> D1
    B1 --> E1
    
    style A1 fill:#4CAF50
    style B1 fill:#2196F3
    style C1 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#F44336
```

### 6.2 Technology Selection Rationale

| Technology | Purpose | Alternatives Considered | Decision Rationale |
|------------|---------|------------------------|-------------------|
| **Java 17** | Programming Language | Kotlin, Scala | LTS support, team expertise, virtual threads roadmap |
| **Spring Boot** | Application Framework | Quarkus, Micronaut | Ecosystem maturity, enterprise support, productivity |
| **PostgreSQL** | Primary Database | MySQL, Oracle | JSONB support, COPY protocol, open source |
| **LMDB** | Edge Cache | RocksDB, Redis | Memory-mapped I/O, zero-copy, embedded |
| **Resilience4j** | Fault Tolerance | Hystrix (deprecated), Sentinel | Lightweight, functional, Spring integration |
| **Micrometer** | Metrics | Dropwizard Metrics | Vendor-neutral, Spring Boot native |
| **Prometheus** | Metrics Store | InfluxDB, Datadog | Pull model, PromQL, open source |
| **Grafana** | Visualization | Kibana, Chronograf | Flexibility, plugin ecosystem, community |

### 6.3 Dependency Management

```yaml
Key Dependencies:
  Spring Boot: 3.2.x
    - spring-boot-starter-web
    - spring-boot-starter-data-jpa
    - spring-boot-starter-actuator
  
  Database:
    - postgresql: 42.7.x
    - HikariCP: 5.1.x (transitive)
    - lmdbjava: 0.9.29
  
  Resilience:
    - resilience4j-spring-boot3: 2.1.x
    - spring-retry: 2.0.x
  
  Utilities:
    - lombok: 1.18.x
    - guava: 32.x
    - caffeine: 3.1.x
  
  Testing:
    - junit-jupiter: 5.10.x
    - mockito-core: 5.x
    - testcontainers: 1.19.x
```

---

## 7. Data Architecture

### 7.1 Conceptual Data Model

```mermaid
erDiagram
    DOMAIN ||--o{ MATCHING_GROUP : contains
    MATCHING_GROUP ||--o{ NODE : contains
    MATCHING_GROUP ||--o{ POTENTIAL_MATCH : generates
    MATCHING_GROUP ||--|| NODES_CURSOR : tracks
    NODE ||--o{ MATCH_PARTICIPATION_HISTORY : records
    MATCHING_GROUP ||--o{ MATCHING_CONFIGURATION : configures
    MATCHING_CONFIGURATION ||--|| ALGORITHM : uses
    
    DOMAIN {
        uuid id PK
        string name
        boolean active
        timestamp created_at
    }
    
    MATCHING_GROUP {
        uuid id PK
        uuid domain_id FK
        string name
        string industry
        boolean active
    }
    
    NODE {
        uuid id PK
        uuid group_id FK
        uuid domain_id FK
        string reference_id UK
        string type
        jsonb metadata
        boolean processed
        timestamp created_at
    }
    
    POTENTIAL_MATCH {
        uuid id PK
        uuid group_id FK
        uuid domain_id FK
        string processing_cycle_id
        string reference_id
        string matched_reference_id
        float compatibility_score
        timestamp matched_at
    }
    
    NODES_CURSOR {
        uuid group_id PK
        uuid domain_id PK
        timestamp cursor_created_at
        uuid cursor_id
        timestamp updated_at
    }
    
    MATCH_PARTICIPATION_HISTORY {
        bigint id PK
        uuid node_id FK
        uuid group_id FK
        uuid domain_id FK
        string processing_cycle_id
        timestamp participated_at
    }
    
    MATCHING_CONFIGURATION {
        uuid id PK
        uuid group_id FK
        uuid algorithm_id FK
        int priority
        jsonb config_params
    }
    
    ALGORITHM {
        string id PK
        string name
        string description
        string strategy_class
    }
```

### 7.2 Data Flow Architecture

```mermaid
flowchart TB
    subgraph "Data Sources"
        A1[External Systems] -->|REST API| A2[Node Ingestion Service]
    end
    
    subgraph "Operational Store - PostgreSQL"
        B1[(nodes<br/>Reference data)]
        B2[(nodes_cursor<br/>Position tracking)]
        B3[(matching_configuration<br/>Strategy config)]
    end
    
    subgraph "Processing Layer"
        C1[Potential Matches System]
        C2[Graph Engine]
        C3[Queue Manager]
    end
    
    subgraph "Cache Layer - LMDB"
        D1[(Edge Store<br/>Temporary cache)]
        D2[(LSH Buckets<br/>Similarity index)]
    end
    
    subgraph "Persistence Layer - PostgreSQL"
        E1[(potential_matches<br/>Final results)]
        E2[(match_participation_history<br/>Audit trail)]
    end
    
    subgraph "Consumption Layer"
        F1[Recommendation Service]
        F2[Analytics Dashboards]
        F3[Reporting System]
    end
    
    A2 --> B1
    B1 --> C1
    B2 --> C1
    B3 --> C1
    
    C1 --> C2
    C2 --> C3
    C3 --> D1
    C2 --> D2
    
    D1 --> E1
    C3 --> E1
    C1 --> E2
    
    E1 --> F1
    E1 --> F2
    E1 --> F3
    
    style B1 fill:#E3F2FD
    style D1 fill:#FFF9C4
    style E1 fill:#C8E6C9
```

### 7.3 Data Volumes & Growth

```mermaid
graph LR
    subgraph "Current State"
        CS1[Nodes: 125K<br/>Matches: 50M<br/>Storage: 200GB]
    end
    
    subgraph "12 Months"
        T1[Nodes: 2M<br/>Matches: 1B<br/>Storage: 2TB]
    end
    
    subgraph "24 Months"
        T2[Nodes: 20M<br/>Matches: 50B<br/>Storage: 20TB]
    end
    
    CS1 -->|16x| T1
    T1 -->|10x| T2
    
    style CS1 fill:#E3F2FD
    style T1 fill:#FFF9C4
    style T2 fill:#FFCCBC
```

**Storage Strategy**:

| Data Type | Retention | Archival | Backup |
|-----------|-----------|----------|--------|
| **Nodes** | Indefinite | N/A | Daily full + WAL |
| **Potential Matches** | 90 days active | Annual archive | Daily incremental |
| **LMDB Edges** | 7 days | Delete on finalize | On-demand snapshot |
| **Cursor State** | Indefinite | N/A | Included in DB backup |
| **Audit Logs** | 1 year | Cold storage | Weekly |

### 7.4 Data Quality & Governance

```mermaid
graph TB
    subgraph "Data Quality Rules"
        Q1[Completeness<br/>No null required fields]
        Q2[Uniqueness<br/>No duplicate matches]
        Q3[Accuracy<br/>Score range 0.0-1.0]
        Q4[Timeliness<br/>Process within 24h]
    end
    
    subgraph "Governance Policies"
        G1[Access Control<br/>Role-based permissions]
        G2[Data Lineage<br/>Processing cycle tracking]
        G3[Audit Trail<br/>All changes logged]
        G4[Retention Policy<br/>90-day active data]
    end
    
    subgraph "Compliance"
        C1[GDPR<br/>Right to erasure]
        C2[Data Privacy<br/>PII masking in logs]
        C3[Security<br/>Encryption at rest]
    end
    
    Q1 --> G1
    Q2 --> G2
    Q3 --> G3
    Q4 --> G4
    
    G1 --> C1
    G2 --> C2
    G3 --> C3
    
    style Q1 fill:#C8E6C9
    style G1 fill:#BBDEFB
    style C1 fill:#FFF9C4
```

---

## 8. Integration Architecture

### 8.1 Integration Landscape

```mermaid
graph TB
    subgraph "Upstream Systems"
        U1[Node Ingestion Service]
        U2[Configuration Service]
        U3[Scheduler Service]
    end
    
    subgraph "Potential Matches System"
        PMS[Core Application]
    end
    
    subgraph "Downstream Systems"
        D1[Recommendation Engine]
        D2[Analytics Platform]
        D3[Reporting Service]
        D4[Notification Service]
    end
    
    subgraph "Infrastructure Services"
        I1[Prometheus Metrics]
        I2[ELK Logging]
        I3[Distributed Tracing Future]
    end
    
    U1 -->|Writes Nodes| PMS
    U2 -->|Provides Config| PMS
    U3 -->|Triggers Jobs| PMS
    
    PMS -->|Publishes Matches| D1
    PMS -->|Exports Data| D2
    PMS -->|Generates Reports| D3
    PMS -->|Sends Alerts Future| D4
    
    PMS -->|Metrics| I1
    PMS -->|Logs| I2
    PMS -.->|Traces| I3
    
    style PMS fill:#4CAF50
    style U1 fill:#2196F3
    style D1 fill:#FF9800
    style I1 fill:#9C27B0
```

### 8.2 Integration Patterns

| Integration Point | Pattern | Protocol | Frequency |
|-------------------|---------|----------|-----------|
| **Node Ingestion → PostgreSQL** | Database Integration | JDBC | Real-time |
| **Scheduler → Application** | Event-Driven | In-process (Spring) | Daily (11:05 IST) |
| **Application → PostgreSQL** | Database Integration | JDBC + COPY | Batch (every 5 sec) |
| **Application → LMDB** | Embedded Database | Memory-mapped | Continuous |
| **Application → Prometheus** | Push/Pull | HTTP | Every 15 sec |
| **Application → Configuration** | Request-Response | HTTP/REST | On startup |
| **PostgreSQL → Analytics** | ETL | SQL Query | Hourly |

### 8.3 API Contracts

#### 8.3.1 Internal Interfaces

```java
// Node Fetch Interface
public interface NodeFetchService {
    CompletableFuture<CursorPage> fetchNodeIdsByCursor(
        UUID groupId, UUID domainId, int limit, String cycleId);
    
    CompletableFuture<List<NodeDTO>> fetchNodesInBatchesAsync(
        List<UUID> nodeIds, UUID groupId, LocalDateTime createdAfter);
    
    void markNodesAsProcessed(List<UUID> nodeIds, UUID groupId);
}

// Match Processing Interface
public interface PotentialMatchService {
    CompletableFuture<NodesCount> processNodeBatch(
        List<UUID> nodeIds, MatchingRequest request);
}

// Storage Interface
public interface PotentialMatchSaver {
    CompletableFuture<Void> saveMatchesAsync(
        List<PotentialMatchEntity> matches, 
        UUID groupId, UUID domainId, String processingCycleId, boolean finalize);
}
```

#### 8.3.2 Database Contracts

**Node Table Schema** (Version 1.0):
```sql
CREATE TABLE nodes (
    id UUID PRIMARY KEY,
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    reference_id VARCHAR(255) NOT NULL,
    type VARCHAR(50),
    metadata JSONB,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    UNIQUE(group_id, domain_id, reference_id)
);
```

**Match Table Schema** (Version 1.0):
```sql
CREATE TABLE potential_matches (
    id UUID PRIMARY KEY,
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    processing_cycle_id VARCHAR(255),
    reference_id VARCHAR(255) NOT NULL,
    matched_reference_id VARCHAR(255) NOT NULL,
    compatibility_score FLOAT NOT NULL,
    matched_at TIMESTAMP NOT NULL,
    UNIQUE(group_id, reference_id, matched_reference_id)
);
```

---

## 11. Scalability & Performance

### 11.1 Scalability Dimensions

```mermaid
graph TB
    subgraph "Vertical Scaling"
        V1[Increase Instance Size<br/>8 CPU → 16 CPU]
        V2[Increase Memory<br/>16GB → 32GB]
        V3[Faster Storage<br/>SSD → NVMe]
    end
    
    subgraph "Horizontal Scaling"
        H1[Domain Partitioning<br/>Route by domainId]
        H2[Multiple Instances<br/>Active-Active]
        H3[Database Read Replicas<br/>5 replicas]
    end
    
    subgraph "Architectural Scaling"
        A1[Distributed Processing<br/>Kafka Workers]
        A2[Sharded Database<br/>By domain/group]
        A3[Distributed Cache<br/>Redis Cluster]
    end
    
    V1 -.->|Current approach| H1
    H1 -.->|Future state| A1
    
    style V1 fill:#E3F2FD
    style H1 fill:#FFF9C4
    style A1 fill:#C8E6C9
```

### 11.2 Performance Optimization Strategies

```mermaid
graph LR
    subgraph "Input Optimization"
        I1[Cursor Pagination<br/>No OFFSET penalty]
        I2[Batch Fetching<br/>1000 nodes/batch]
        I3[Connection Pooling<br/>20 connections]
    end
    
    subgraph "Processing Optimization"
        P1[LSH Indexing<br/>O n log n vs O n^2]
        P2[Parallel Workers<br/>8 concurrent threads]
        P3[Memory Queues<br/>Avoid disk I/O]
    end
    
    subgraph "Output Optimization"
        O1[Binary COPY Protocol<br/>10x faster INSERT]
        O2[Batch Writes<br/>50K records/batch]
        O3[LMDB Single Writer<br/>No contention]
    end
    
    I1 --> P1
    I2 --> P2
    I3 --> P3
    P1 --> O1
    P2 --> O2
    P3 --> O3
    
    style I1 fill:#C8E6C9
    style P1 fill:#BBDEFB
    style O1 fill:#FFF9C4
```

### 11.3 Scaling Roadmap

```mermaid
gantt
    title Scalability Roadmap
    dateFormat YYYY-MM-DD
    
    section Phase 1: Optimization
    Cursor Pagination          :done, p1-1, 2024-01-01, 30d
    LSH Implementation         :done, p1-2, 2024-02-01, 60d
    Binary COPY Protocol       :done, p1-3, 2024-03-01, 20d
    
    section Phase 2: Vertical Scaling
    Increase Instance Size     :active, p2-1, 2024-06-01, 15d
    Add Read Replicas          :p2-2, 2024-07-01, 30d
    Optimize Thread Pools      :p2-3, 2024-08-01, 20d
    
    section Phase 3: Horizontal Scaling
    Domain Partitioning        :p3-1, 2024-09-01, 60d
    Active-Active Setup        :p3-2, 2024-11-01, 45d
    Load Balancing             :p3-3, 2025-01-01, 30d
    
    section Phase 4: Distributed Architecture
    Kafka Integration          :p4-1, 2025-03-01, 90d
    Worker Pool                :p4-2, 2025-06-01, 60d
    Auto-Scaling               :p4-3, 2025-08-01, 45d
```

**Capacity Planning**:

| Year | Nodes/Day | Matches/Day | Storage | Infrastructure Cost |
|------|-----------|-------------|---------|-------------------|
| **Current** | 50K | 10M | 200GB | $5K/month |
| **Year 1** | 500K | 100M | 2TB | $15K/month |
| **Year 2** | 5M | 1B | 20TB | $50K/month |
| **Year 3** | 50M | 50B | 200TB | $200K/month |

---


## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Node** | An entity (user, product, resource) participating in matching |
| **Edge** | A weighted connection representing compatibility between nodes |
| **Potential Match** | A computed relationship with compatibility score |
| **Cursor** | Position marker for incremental processing |
| **Cycle ID** | Unique identifier for a processing run |
| **LSH** | Locality-Sensitive Hashing - algorithm for similarity search |
| **LMDB** | Lightning Memory-Mapped Database - embedded key-value store |
| **Semaphore** | Concurrency control mechanism limiting parallel execution |
| **Advisory Lock** | PostgreSQL application-level locking |
| **COPY Protocol** | PostgreSQL bulk data loading mechanism |

---

## Appendix B: References

**Internal Documentation**:
- Perfect Match Creation System - Low-Level Design (LLD)
- Database Schema Documentation
- API Documentation (Swagger)
- Operational Runbooks

**External References**:
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [LMDB Documentation](https://lmdb.readthedocs.io/)
- [LSH Algorithm Papers](https://en.wikipedia.org/wiki/Locality-sensitive_hashing)
- [Resilience4j Guide](https://resilience4j.readme.io/)

---


