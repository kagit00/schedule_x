# Potential Matches Creation System - High-Level Design Document


---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#3-system-architecture)
3. [Functional Architecture](#4-functional-architecture)
4. [Technology Stack](#6-technology-stack)
5. [Data Architecture](#7-data-architecture)
6. [Scalability & Performance](#11-scalability--performance)


---

## 1. Executive Summary

### 1.1 System Overview

The **Potential Matches Creation System** is an enterprise-grade graph processing platform designed to compute and persist compatibility relationships between entities at scale. The system processes millions of nodes daily, generating match recommendations using sophisticated algorithms including LSH (Locality-Sensitive Hashing), metadata-based weighting, and flat comparison strategies.




## 3. System Architecture



### 3.1 Logical Architecture

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


