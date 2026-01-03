# Perfect Match Creation System - High-Level Design Document

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#3-system-architecture)
3. [Functional Requirements](#4-functional-requirements)
4. [Technology Stack](#6-technology-stack)
5. [Component Architecture](#7-component-architecture)
6. [Data Architecture](#8-data-architecture)


---

## 1. Executive Summary

### 1.1 System Overview

The **Perfect Match Creation System** is an enterprise-grade, high-throughput graph processing platform designed to compute optimal matches between entities based on compatibility scores. The system processes millions of edges daily, applying sophisticated matching algorithms to generate actionable match recommendations.

### 1.2 Key Capabilities

```mermaid
flowchart TB
    ROOT["Perfect Match<br/>System"]

    ROOT --> BP["Batch Processing"]
    ROOT --> GP["Graph Processing"]
    ROOT --> DM["Data Management"]
    ROOT --> RS["Resilience"]
    ROOT --> SC["Scalability"]

    BP --> BP1["Scheduled Execution"]
    BP --> BP2["Incremental Updates"]
    BP --> BP3["Progress Tracking"]

    GP --> GP1["Edge Streaming"]
    GP --> GP2["Algorithm Selection"]
    GP --> GP3["Compatibility Scoring"]

    DM --> DM1["LMDB Storage"]
    DM --> DM2["PostgreSQL Persistence"]
    DM --> DM3["Metadata Tracking"]

    RS --> RS1["Circuit Breakers"]
    RS --> RS2["Retry Mechanisms"]
    RS --> RS3["Graceful Degradation"]

    SC --> SC1["Concurrent Processing"]
    SC --> SC2["Resource Management"]
    SC --> SC3["Horizontal Scaling"]

    style ROOT fill:#ECEFF1
    style BP fill:#E3F2FD
    style GP fill:#E8F5E9
    style DM fill:#FFFDE7
    style RS fill:#FCE4EC
    style SC fill:#EDE7F6
```


## 3. System Architecture


### 3.1 Architectural Layers

```mermaid
graph TB
    subgraph "Presentation Layer"
        A1[Scheduled Jobs]
        A2[REST APIs<br/>Future]
        A3[Monitoring Endpoints]
    end
    
    subgraph "Application Layer"
        B1[Job Orchestration]
        B2[Workflow Management]
        B3[Business Logic]
    end
    
    subgraph "Domain Layer"
        C1[Matching Algorithms]
        C2[Graph Processing]
        C3[Strategy Selection]
    end
    
    subgraph "Infrastructure Layer"
        D1[LMDB Integration]
        D2[PostgreSQL Access]
        D3[Thread Pool Management]
        D4[Metrics Collection]
    end
    
    subgraph "Cross-Cutting Concerns"
        E1[Security]
        E2[Logging]
        E3[Error Handling]
        E4[Caching]
    end
    
    A1 --> B1
    A2 --> B2
    A3 --> B3
    B1 --> C1
    B2 --> C2
    B3 --> C3
    C1 --> D1
    C2 --> D2
    C3 --> D3
    
    E1 -.-> B1
    E2 -.-> B2
    E3 -.-> C1
    E4 -.-> D1
    
    style A1 fill:#4CAF50
    style B1 fill:#2196F3
    style C1 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#F44336
```

### 3.2 Component Interaction Overview

```mermaid
sequenceDiagram
    autonumber

    participant S as Scheduler
    participant O as Orchestrator
    participant P as Processor
    participant G as GraphEngine
    participant L as LMDB
    participant D as PostgreSQL

    rect rgba(200,230,255,0.35)
        Note over S,O: Initialization Phase
        S->>O: Trigger daily job
        O->>D: Fetch eligible tasks
        D-->>O: Task list (domain, group)
    end

    rect rgba(230,255,200,0.35)
        Note over O,P: Orchestration Phase
        loop For each task
            O->>O: Acquire concurrency permits
            O->>P: Start processing task
        end
    end

    rect rgba(255,240,200,0.35)
        Note over P,L: Processing Phase
        P->>G: Load matching context
        P->>L: Stream graph edges
        L-->>P: Edge data stream
        P->>G: Execute matching algorithm
        G-->>P: Match results
    end

    rect rgba(255,220,230,0.35)
        Note over P,D: Persistence Phase
        P->>D: Bulk insert matches (COPY)
        D-->>P: Insert acknowledgment
        P->>D: Update progress metadata
    end

    rect rgba(230,230,250,0.35)
        Note over O,D: Completion Phase
        P-->>O: Task completed
        O->>O: Release permits
        O->>D: Update job status
    end

```

---

## 4. Functional Requirements

### 4.1 Core Capabilities

#### FR-1: Scheduled Batch Processing
**Description**: System shall execute perfect match creation on a scheduled basis.

**Acceptance Criteria**:
- Execute daily at configured time (01:28 IST)
- Process all eligible domain-group combinations
- Complete within 4-hour SLA window
- Support configurable cron expressions

#### FR-2: Incremental Node Processing
**Description**: System shall process only new nodes since last successful run.

**Acceptance Criteria**:
- Track last processed node count per group
- Compare current node count vs. last run
- Skip processing if no new nodes
- Update metadata upon successful completion

#### FR-3: Multi-Algorithm Support
**Description**: System shall support multiple matching algorithms based on configuration.

**Acceptance Criteria**:
- Symmetric matching (mutual preference)
- Asymmetric matching (one-way preference)
- Algorithm selection per group configuration
- Extensible strategy pattern for new algorithms

#### FR-4: Result Persistence
**Description**: System shall persist match results to PostgreSQL with deduplication.

**Acceptance Criteria**:
- Store matches with compatibility scores
- Prevent duplicate matches (upsert logic)
- Maintain historical match timestamp
- Link to processing cycle ID

#### FR-5: Progress Tracking
**Description**: System shall track processing status and progress.

**Acceptance Criteria**:
- Record job start/end times
- Track PENDING/COMPLETED/FAILED status
- Store processed node count
- Enable restart from failure point

### 4.2 Feature Matrix

| Feature | Priority | Status | Version |
|---------|----------|--------|---------|
| Scheduled Execution | P0 | Complete | 1.0 |
| Incremental Processing | P0 | Complete | 1.0 |
| Symmetric Matching | P0 | Complete | 1.0 |
| Asymmetric Matching | P0 |  Complete | 1.0 |
| Circuit Breaker | P1 |  Complete | 1.0 |
| Retry Mechanism | P1 |  Complete | 1.0 |
| Metrics Export | P1 |  Complete | 1.0 |
| Manual Trigger API | P2 | 📋 Planned | 2.0 |
| Real-time Matching | P2 | 📋 Planned | 2.0 |
| ML-based Scoring | P3 | 💡 Future | 3.0 |

---



## 6. Technology Stack

### 6.1 Technology Landscape

```mermaid
graph TB
    subgraph "Application Layer"
        A1[Java 17 LTS]
        A2[Spring Boot 3.x]
        A3[Spring Framework 6.x]
    end
    
    subgraph "Data Storage"
        B1[PostgreSQL 15]
        B2[LMDB 0.9.x]
        B3[HikariCP Connection Pool]
    end
    
    subgraph "Resilience"
        C1[Resilience4j Circuit Breaker]
        C2[Spring Retry]
        C3[Semaphore-based Rate Limiting]
    end
    
    subgraph "Observability"
        D1[Micrometer Metrics]
        D2[Prometheus]
        D3[Grafana]
        D4[SLF4j + Logback]
    end
    
    subgraph "Build & Deployment"
        E1[Maven 3.9]
        E2[Docker]
        E3[Kubernetes Optional]
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

| Technology | Purpose | Justification |
|------------|---------|---------------|
| **Java 17** | Programming Language | LTS support, virtual threads (future), performance |
| **Spring Boot** | Application Framework | Enterprise features, auto-configuration, ecosystem |
| **PostgreSQL** | Primary Database | ACID compliance, JSONB support, mature ecosystem |
| **LMDB** | Edge Cache | Memory-mapped I/O, zero-copy reads, high performance |
| **Resilience4j** | Fault Tolerance | Lightweight, Spring integration, comprehensive patterns |
| **Micrometer** | Metrics | Vendor-neutral, Spring Boot native, Prometheus compatible |
| **HikariCP** | Connection Pooling | Fastest pool, production-proven, low overhead |

### 6.3 Dependency Management

```yaml
Key Dependencies:
  Spring Boot Starter: 3.2.x
  Spring Data JPA: 3.2.x
  PostgreSQL Driver: 42.7.x
  LMDB Java: 0.9.29
  Resilience4j: 2.1.x
  Micrometer: 1.12.x
  Lombok: 1.18.x
  
Build Tools:
  Maven: 3.9.x
  Java: 17 LTS
  
Testing:
  JUnit 5: 5.10.x
  Mockito: 5.x
  Testcontainers: 1.19.x
```

---

## 7. Component Architecture

### 7.1 Layered Component View

```mermaid
graph TB
    subgraph "Scheduler Layer"
        SL1[PerfectMatchesCreationScheduler]
        SL2[Cron Configuration]
        SL3[Circuit Breaker Wrapper]
    end
    
    subgraph "Service Layer"
        SV1[PerfectMatchCreationService]
        SV2[PerfectMatchServiceImpl]
        SV3[MatchingStrategySelector]
    end
    
    subgraph "Execution Layer"
        EX1[PerfectMatchCreationJobExecutor]
        EX2[Retry Logic Manager]
        EX3[Batch Coordinator]
    end
    
    subgraph "Domain Layer"
        DM1[MatchingStrategy Interface]
        DM2[Symmetric Strategy]
        DM3[Asymmetric Strategy]
        DM4[Graph Processing Logic]
    end
    
    subgraph "Data Access Layer"
        DA1[EdgePersistence Facade]
        DA2[PerfectMatchSaver]
        DA3[Repository Layer]
        DA4[LMDB Reader]
    end
    
    subgraph "Infrastructure Layer"
        IN1[Thread Pool Executors]
        IN2[Semaphore Management]
        IN3[Metrics Registry]
        IN4[Connection Pools]
    end
    
    SL1 --> SV1
    SV1 --> EX1
    EX1 --> SV2
    SV2 --> DM1
    SV2 --> DA1
    DM1 --> DM2
    DM1 --> DM3
    DM2 --> DM4
    DA1 --> DA4
    DA1 --> DA2
    DA2 --> DA3
    
    SV1 -.-> IN2
    EX1 -.-> IN1
    DA2 -.-> IN4
    SV2 -.-> IN3
    
    style SL1 fill:#E8F5E9
    style SV1 fill:#E3F2FD
    style EX1 fill:#FFF9C4
    style DM1 fill:#F3E5F5
    style DA1 fill:#FFEBEE
    style IN1 fill:#E0F2F1
```

### 7.2 Component Responsibility Matrix

| Component | Responsibility | Input | Output |
|-----------|---------------|-------|--------|
| **PerfectMatchesCreationScheduler** | Job triggering, resilience | Cron trigger | Task list execution |
| **PerfectMatchCreationService** | Resource orchestration, concurrency control | Group IDs | Processing futures |
| **PerfectMatchCreationJobExecutor** | Retry management, error handling | Group metadata | Match results |
| **PerfectMatchServiceImpl** | Core processing logic, streaming | Match requests | Saved matches |
| **MatchingStrategySelector** | Algorithm selection | Context | Strategy instance |
| **EdgePersistence** | LMDB abstraction | Query params | Edge streams |
| **PerfectMatchSaver** | Async save orchestration | Match entities | Completion futures |
| **PerfectMatchStorageProcessor** | DB write optimization | Entity batches | Persisted records |

### 7.3 Communication Patterns

```mermaid
graph LR
    subgraph "Synchronous"
        A1[Repository Queries]
        A2[Strategy Selection]
        A3[Configuration Loading]
    end
    
    subgraph "Asynchronous"
        B1[Batch Processing<br/>CompletableFuture]
        B2[Match Saving<br/>Non-blocking I/O]
        B3[Metric Collection<br/>Fire-and-forget]
    end
    
    subgraph "Event-Driven"
        C1[Job Completion Events]
        C2[Error Callbacks]
        C3[Progress Updates]
    end
    
    A1 -.->|Blocking| DB[(Database)]
    B1 -.->|Async| EXEC[Thread Pool]
    C1 -.->|Publish| METRICS[Metrics System]
    
    style A1 fill:#FFCDD2
    style B1 fill:#C8E6C9
    style C1 fill:#BBDEFB
```

---

## 8. Data Architecture

### 8.1 Conceptual Data Model

```mermaid
erDiagram
    DOMAIN ||--o{ MATCHING_GROUP : contains
    MATCHING_GROUP ||--o{ MATCHING_CONFIGURATION : has
    MATCHING_CONFIGURATION ||--|| ALGORITHM : uses
    MATCHING_GROUP ||--o{ NODE : contains
    MATCHING_GROUP ||--o{ EDGE : contains
    MATCHING_GROUP ||--o{ PERFECT_MATCH : generates
    MATCHING_GROUP ||--|| LAST_RUN : tracks
    
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
        boolean cost_based
        string industry
        boolean active
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
        string strategy_class
    }
    
    NODE {
        uuid id PK
        uuid group_id FK
        uuid domain_id FK
        string reference_id
        boolean processed
        timestamp created_at
    }
    
    EDGE {
        uuid id PK
        uuid group_id FK
        uuid domain_id FK
        string from_node_hash
        string to_node_hash
        float score
        string cycle_id
    }
    
    PERFECT_MATCH {
        uuid id PK
        uuid group_id FK
        uuid domain_id FK
        string processing_cycle_id
        string reference_id
        string matched_reference_id
        float compatibility_score
        timestamp matched_at
    }
    
    LAST_RUN {
        uuid id PK
        uuid group_id FK
        uuid domain_id FK
        bigint node_count
        string status
        timestamp run_date
    }
```

### 8.2 Data Flow Architecture

```mermaid
flowchart TB
    subgraph "Data Ingestion"
        A1[External Systems] -->|REST API| A2[Node Ingestion Service]
        A2 --> A3[(PostgreSQL<br/>Nodes Table)]
    end
    
    subgraph "Edge Computation"
        A3 -->|Read Nodes| B1[Similarity Computation Service]
        B1 -->|Compute Scores| B2[Edge Generator]
        B2 -->|Write| B3[(LMDB<br/>Edge Store)]
    end
    
    subgraph "Match Processing"
        B3 -->|Stream| C1[Perfect Match System]
        A3 -->|Node Metadata| C1
        C1 -->|Algorithm Selection| C2[Matching Engine]
        C2 -->|Generate| C3[Match Results]
    end
    
    subgraph "Data Persistence"
        C3 -->|Bulk Insert| D1[(PostgreSQL<br/>Perfect Matches)]
        C1 -->|Update Status| D2[(PostgreSQL<br/>Last Run Metadata)]
    end
    
    subgraph "Data Consumption"
        D1 -->|Query| E1[Recommendation Service]
        D1 -->|Analytics| E2[BI Dashboards]
        D1 -->|Export| E3[Data Warehouse]
    end
    
    style A3 fill:#E3F2FD
    style B3 fill:#FFF9C4
    style D1 fill:#C8E6C9
    style D2 fill:#FFEBEE
```

### 8.3 Storage Strategy

| Data Type | Storage | Retention | Backup | Access Pattern |
|-----------|---------|-----------|--------|----------------|
| **Nodes** | PostgreSQL | Indefinite | Daily | Read-heavy |
| **Edges** | LMDB | 7 days | On-demand | Read-only streaming |
| **Perfect Matches** | PostgreSQL | 90 days | Daily | Write-once, read-many |
| **Metadata** | PostgreSQL | Indefinite | Daily | Low volume |
| **Metrics** | Prometheus | 30 days | None | Time-series queries |
| **Logs** | ELK Stack | 14 days | None | Search & analytics |

### 8.4 Data Volumes

```mermaid
pie title Data Volume Distribution
    "Edges (LMDB)" : 60
    "Perfect Matches (PostgreSQL)" : 25
    "Nodes (PostgreSQL)" : 10
    "Metadata (PostgreSQL)" : 3
    "Metrics (Prometheus)" : 2
```

**Estimated Growth**:
- Current: ~50GB LMDB, 200GB PostgreSQL
- 12 months: ~200GB LMDB, 800GB PostgreSQL
- 24 months: ~1TB LMDB, 4TB PostgreSQL

---


## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Edge** | A weighted connection between two nodes representing compatibility |
| **Node** | An entity (user, product, resource) participating in matching |
| **Matching Algorithm** | Strategy for determining optimal matches (Symmetric/Asymmetric) |
| **Perfect Match** | The computed result representing best compatibility between entities |
| **Cycle ID** | Unique identifier for a processing run |
| **LMDB** | Lightning Memory-Mapped Database - high-performance key-value store |
| **Circuit Breaker** | Resilience pattern to prevent cascading failures |
| **Semaphore** | Concurrency control mechanism limiting parallel execution |
| **Advisory Lock** | PostgreSQL locking mechanism for application-level coordination |

---

## Appendix B: Reference Architecture

### System Capabilities Summary

```mermaid
graph TB
    ROOT["Perfect Match System"]

    %% Top-level categories
    ROOT --> F["Functional"]
    ROOT --> NF["Non Functional"]
    ROOT --> OP["Operational"]

    %% Functional
    F --> F1["Scheduled Processing"]
    F --> F2["Incremental Updates"]
    F --> F3["Multi Algorithm Support"]
    F --> F4["Result Persistence"]

    %% Non-Functional
    NF --> NFP["High Performance"]
    NF --> NFR["High Reliability"]
    NF --> NFS["Scalability"]
    NF --> NFSec["Security"]

    %% Performance details
    NFP --> NFP1["500K edges per minute"]
    NFP --> NFP2["Less than 15 min latency"]

    %% Reliability details
    NFR --> NFR1["99.5 percent uptime"]
    NFR --> NFR2["Auto retry"]

    %% Scalability details
    NFS --> NFS1["Horizontal scaling ready"]
    NFS --> NFS2["Resource efficient"]

    %% Security details
    NFSec --> NFSec1["Encryption at rest and transit"]
    NFSec --> NFSec2["Audit logging"]

    %% Operational
    OP --> OPMon["Monitoring"]
    OP --> OPLog["Logging"]
    OP --> OPMaint["Maintenance"]

    %% Monitoring details
    OPMon --> OPMon1["Prometheus metrics"]
    OPMon --> OPMon2["Grafana dashboards"]

    %% Logging details
    OPLog --> OPLog1["Centralized ELK"]
    OPLog --> OPLog2["Structured logs"]

    %% Maintenance details
    OPMaint --> OPM1["Zero downtime deploy"]
    OPMaint --> OPM2["Automated backups"]

    %% Styling
    style ROOT fill:#4CAF50,color:#ffffff
    style F fill:#E3F2FD
    style NF fill:#FFF3E0
    style OP fill:#E8F5E9

```


---

