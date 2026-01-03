# Potential Matches Creation System - High-Level Design Document



---

This document describes the architectural design of a batch-oriented potential-match computation pipeline developed as part of an independent backend systems project. The focus is on system structure, algorithmic flow, and correctness rather than production deployment, tuning, or capacity planning.

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#3-system-architecture)
3. [Functional Architecture](#4-functional-architecture)
4. [Technology Stack](#6-technology-stack)
5. [Data Architecture](#7-data-architecture)

---

## 1. Executive Summary

### 1.1 System Overview

The **Potential Matches Creation System** is a batch-oriented graph processing design for computing and persisting compatibility relationships between entities using strategies such as Locality-Sensitive Hashing (LSH), metadata weighting, and flat comparison.

---

## 3. System Architecture

### 3.1 Logical Architecture

```mermaid
graph TB
    subgraph Presentation_Layer
        A1[Scheduled Triggers\n@Scheduled Cron]
        A2[Manual API\nConceptual]
    end
    
    subgraph Application_Services_Layer
        B1[Orchestration Service\nJob Scheduling & Coordination]
        B2[Processing Service\nBusiness Logic]
        B3[Node Management Service\nEntity Lifecycle]
    end
    
    subgraph Domain_Layer
        C1[Graph Processing\nAlgorithms & Strategies]
        C2[Match Computation\nScoring & Ranking]
        C3[Queue Management\nBuffering & Flow Control]
    end
    
    subgraph Infrastructure_Layer
        D1[Data Access\nRepositories & DAOs]
        D2[Storage Abstraction\nLMDB + PostgreSQL]
        D3[Concurrency Control\nSemaphores & Thread Pools]
        D4[Instrumentation]
    end
    
    subgraph External_Systems
        E1[(PostgreSQL\nMaster Database)]
        E2[(LMDB\nEdge Cache)]
        E3[Configuration]
    end
    
    A1 --> B1
    A2 --> B2
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
    
    C1 --> D1
    C2 --> D2
    C3 --> D3
    
    D1 --> E1
    D2 --> E2
    B1 --> E3
    
    style A1 fill:#4CAF50
    style B1 fill:#2196F3
    style C1 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#607D8B

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
    
    G --> H[Fetch Node Batch]
    H --> I{Nodes Found?}
    
    I -->|No| J[Empty Streak++]
    J --> K{Streak Exceeded?}
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
    
    G --> I{Node Count<br/>Threshold?}
    H --> I
    
    I -->|Yes| J[Use LSH Strategy<br/>Sub-linear complexity]
    I -->|No| K[Use Metadata Strategy<br/>Full comparison]
    
    D --> L[Index Nodes<br/>Simple list]
    J --> M[Index Nodes<br/>Build LSH buckets]
    K --> N[Index Nodes<br/>Metadata encoding]
    
    L --> O[Process Batch<br/>Pairwise comparison]
    M --> P[Process Batch<br/>Via LSH]
    N --> Q[Process Batch<br/>Weighted]
    
    O --> R[Generate Matches]
    P --> R
    Q --> R
    
    style D fill:#FFCDD2
    style J fill:#C8E6C9
    style K fill:#BBDEFB
    style R fill:#FFF9C4
```

---

## 6. Technology Stack

### 6.1 Technology Landscape

```mermaid
graph TB
    subgraph "Application Tier"
        A1[Java]
        A2[Spring Boot]
        A3[Spring Framework]
    end
    
    subgraph "Data Tier"
        B1[PostgreSQL]
        B2[LMDB]
        B3[Connection Pooling]
    end
    
    subgraph "Resilience Tier"
        C1[Resilience4j]
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
| **Java** | Programming Language | Long-term support and ecosystem |
| **Spring Boot** | Application Framework | Enterprise features and productivity |
| **PostgreSQL** | Primary Database | ACID compliance and extensions |
| **LMDB** | Edge Cache | Memory-mapped I/O and read performance |
| **Resilience4j** | Fault Tolerance | Lightweight and comprehensive patterns |

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
        A1[External Systems] -->|Input| A2[Node Ingestion Service]
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
        F2[Analytics Platforms]
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

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Node** | An entity participating in matching |
| **Edge** | A weighted connection representing compatibility between nodes |
| **Potential Match** | A computed relationship with compatibility score |
| **Cursor** | Position marker for incremental processing |
| **Cycle ID** | Unique identifier for a processing run |
| **LSH** | Locality-Sensitive Hashing - algorithm for similarity search |
| **LMDB** | Lightning Memory-Mapped Database - embedded key-value store |
| **Semaphore** | Concurrency control mechanism limiting parallel execution |
| **Advisory Lock** | Database application-level locking |
| **COPY Protocol** | PostgreSQL bulk data loading mechanism |

---

## Appendix B: References

**Design Documentation**:
- Potential Matches Creation System - Low-Level Design (LLD)
- Database Schema Documentation
- API Documentation

**External References**:
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [LMDB Documentation](https://lmdb.readthedocs.io/)
- [Locality-Sensitive Hashing](https://en.wikipedia.org/wiki/Locality-sensitive_hashing)
- [Resilience4j Guide](https://resilience4j.readme.io/)