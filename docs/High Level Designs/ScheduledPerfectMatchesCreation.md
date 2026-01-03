# Perfect Match Creation System - High-Level Design Document


---

This document describes the architectural design of a batch-oriented graph processing system developed as part of an independent backend systems project. The focus is on system structure, data flow, algorithm selection, and correctness rather than production deployment or operational characteristics.

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

The **Perfect Match Creation System** is a batch-oriented graph processing system designed to generate optimal matches between entities based on compatibility scores.

### 1.2 Key Capabilities

```mermaid
flowchart TB
    ROOT["Perfect Match<br/>System"]

    ROOT --> BP["Batch Processing"]
    ROOT --> GP["Graph Processing"]
    ROOT --> DM["Data Management"]
    ROOT --> RS["Resilience"]

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

    style ROOT fill:#ECEFF1
    style BP fill:#E3F2FD
    style GP fill:#E8F5E9
    style DM fill:#FFFDE7
    style RS fill:#FCE4EC
```

---

## 3. System Architecture

### 3.1 Architectural Layers

```mermaid
graph TB
    subgraph Presentation_Layer
        A1[Scheduled Jobs]
        A2[REST APIs\nConceptual]
        A3[Monitoring Endpoints]
    end
    
    subgraph Application_Layer
        B1[Job Orchestration]
        B2[Workflow Management]
        B3[Business Logic]
    end
    
    subgraph Domain_Layer
        C1[Matching Algorithms]
        C2[Graph Processing]
        C3[Strategy Selection]
    end
    
    subgraph Infrastructure_Layer
        D1[LMDB Integration]
        D2[PostgreSQL Access]
        D3[Thread Pool Management]
        D4[Instrumentation]
    end
    
    subgraph Cross_Cutting_Concerns
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
        S->>O: Trigger job
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
**Description**: The system executes perfect match creation on a scheduled basis.

**Acceptance Criteria**:
- Executes according to configurable schedule
- Processes eligible domain-group combinations
- Supports configurable cron expressions

#### FR-2: Incremental Node Processing
**Description**: The system processes only new nodes since the last successful run.

**Acceptance Criteria**:
- Tracks last processed node count per group
- Compares current node count against previous run
- Skips processing if no new nodes
- Updates metadata upon successful completion

#### FR-3: Multi-Algorithm Support
**Description**: The system supports multiple matching algorithms based on configuration.

**Acceptance Criteria**:
- Symmetric matching (mutual preference)
- Asymmetric matching (one-way preference)
- Algorithm selection per group configuration
- Extensible strategy pattern for new algorithms

#### FR-4: Result Persistence
**Description**: The system persists match results to PostgreSQL with deduplication.

**Acceptance Criteria**:
- Stores matches with compatibility scores
- Prevents duplicate matches (upsert logic)
- Maintains historical match timestamp
- Links to processing cycle identifier

#### FR-5: Progress Tracking
**Description**: The system tracks processing status and progress.

**Acceptance Criteria**:
- Records job start/end times
- Tracks status states
- Stores processed node count
- Supports restart from failure point

### 4.2 Feature Coverage

| Feature | Priority | Design Coverage | Notes |
|---------|----------|-----------------|-------|
| Scheduled Execution | P0 | Supported | Configurable scheduling |
| Incremental Processing | P0 | Supported | Metadata-based tracking |
| Symmetric Matching | P0 | Supported | Mutual preference algorithm |
| Asymmetric Matching | P0 | Supported | One-way preference algorithm |
| Circuit Breaker | P1 | Supported | Resilience pattern |
| Retry Mechanism | P1 | Supported | Automated retries |
| Manual Trigger API | P2 | Planned | On-demand execution |
| Real-time Matching | P2 | Planned | Event-driven processing |
| ML-based Scoring | P3 | Planned | Future enhancement |

---

## 6. Technology Stack

### 6.1 Technology Landscape

```mermaid
graph TB
    subgraph "Application Layer"
        A1[Java]
        A2[Spring Boot]
        A3[Spring Framework]
    end
    
    subgraph "Data Storage"
        B1[PostgreSQL]
        B2[LMDB]
        B3[Connection Pooling]
    end
    
    subgraph "Resilience"
        C1[Resilience4j Circuit Breaker]
        C2[Retry Support]
        C3[Concurrency Control]
    end
    
    subgraph "Instrumentation (Design Intent)"
        D1[Metrics Framework]
        D2[Structured Logging]
    end
    
    style A1 fill:#4CAF50
    style B1 fill:#2196F3
    style C1 fill:#FF9800
    style D1 fill:#9C27B0
```

### 6.2 Technology Selection Rationale

| Technology | Purpose | Justification |
|------------|---------|---------------|
| **Java** | Programming Language | Long-term support and performance |
| **Spring Boot** | Application Framework | Enterprise features and ecosystem |
| **PostgreSQL** | Primary Database | ACID compliance and extensions |
| **LMDB** | Edge Cache | Memory-mapped I/O and read performance |
| **Resilience4j** | Fault Tolerance | Lightweight and comprehensive patterns |

---

## 7. Component Architecture

### 7.1 Layered Component View

```mermaid
graph TB
    subgraph "Scheduler Layer"
        SL1[PerfectMatchesCreationScheduler]
        SL2[Configuration]
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
        IN2[Concurrency Management]
        IN3[Instrumentation]
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
| **PerfectMatchesCreationScheduler** | Job triggering, resilience | Configuration | Task list execution |
| **PerfectMatchCreationService** | Resource orchestration, concurrency control | Group identifiers | Processing coordination |
| **PerfectMatchCreationJobExecutor** | Retry management, error handling | Group metadata | Match results |
| **PerfectMatchServiceImpl** | Core processing logic, streaming | Match requests | Saved matches |
| **MatchingStrategySelector** | Algorithm selection | Context | Strategy instance |
| **EdgePersistence** | LMDB abstraction | Query parameters | Edge streams |
| **PerfectMatchSaver** | Save orchestration | Match entities | Completion status |
| **PerfectMatchStorageProcessor** | Write optimization | Entity batches | Persisted records |

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
        B2[Match Saving<br/>Non-blocking]
        B3[Instrumentation<br/>Fire-and-forget]
    end
    
    subgraph "Event-Driven"
        C1[Job Completion Events]
        C2[Error Callbacks]
        C3[Progress Updates]
    end
    
    A1 -.->|Blocking| DB[(Database)]
    B1 -.->|Async| EXEC[Thread Pool]
    C1 -.->|Publish| INSTR[Instrumentation]
    
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
        A1[External Systems] -->|Input| A2[Node Ingestion Service]
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
        D1 -->|Analytics| E2[Analytics Platforms]
        D1 -->|Export| E3[Data Consumers]
    end
    
    style A3 fill:#E3F2FD
    style B3 fill:#FFF9C4
    style D1 fill:#C8E6C9
    style D2 fill:#FFEBEE
```

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Edge** | A weighted connection between two nodes representing compatibility |
| **Node** | An entity participating in matching |
| **Matching Algorithm** | Strategy for determining optimal matches (Symmetric/Asymmetric) |
| **Perfect Match** | The computed result representing best compatibility between entities |
| **Cycle ID** | Unique identifier for a processing run |
| **LMDB** | Lightning Memory-Mapped Database - high-performance key-value store |
| **Circuit Breaker** | Resilience pattern to prevent cascading failures |
| **Semaphore** | Concurrency control mechanism limiting parallel execution |
| **Advisory Lock** | Database locking mechanism for application-level coordination |