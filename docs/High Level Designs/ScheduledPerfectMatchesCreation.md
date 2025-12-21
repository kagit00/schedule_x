# Perfect Match Creation System - High-Level Design Document

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#3-system-architecture)
3. [Functional Requirements](#4-functional-requirements)
4. [Non-Functional Requirements](#5-non-functional-requirements)
5. [Technology Stack](#6-technology-stack)
6. [Component Architecture](#7-component-architecture)
7. [Data Architecture](#8-data-architecture)
8. [Security Architecture](#11-security-architecture)
9. [Scalability & Performance](#12-scalability--performance)




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

### 3.1 Logical Architecture

```mermaid
C4Context
    title System Context Diagram - Perfect Match Creation System

    Person(ops, "Operations Team", "Monitors system health")
    Person(bizuser, "Business Users", "Consumes match results")
    
    System(pms, "Perfect Match System", "Computes optimal entity matches")
    
    System_Ext(nodeingestion, "Node Ingestion Service", "Adds new entities")
    System_Ext(edgecompute, "Edge Computation Service", "Calculates compatibility scores")
    SystemDb_Ext(lmdb, "LMDB Storage", "High-performance edge cache")
    SystemDb_Ext(postgres, "PostgreSQL", "Master data store")
    System_Ext(monitoring, "Monitoring Stack", "Prometheus + Grafana")
    
    Rel(nodeingestion, pms, "Triggers processing")
    Rel(edgecompute, lmdb, "Writes edges")
    Rel(pms, lmdb, "Reads edges")
    Rel(pms, postgres, "Writes/Reads matches")
    Rel(pms, monitoring, "Exports metrics")
    Rel(ops, monitoring, "Views dashboards")
    Rel(bizuser, postgres, "Queries results")
    
    UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="2")
```

### 3.2 Architectural Layers

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

### 3.3 Component Interaction Overview

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

## 5. Non-Functional Requirements

### 5.1 Performance Requirements

```mermaid
graph LR
    subgraph "Performance SLAs"
        A[Throughput<br/>≥ 500K edges/min]
        B[Latency<br/>≤ 15 min end-to-end]
        C[Concurrency<br/>2 domains × 1 group]
        D[Memory<br/>≤ 8GB heap]
    end
    
    subgraph "Optimization Strategies"
        E[Streaming<br/>No full graph load]
        F[Batching<br/>25K edge chunks]
        G[Async I/O<br/>Non-blocking saves]
        H[Connection Pooling<br/>20 DB connections]
    end
    
    A --> E
    B --> F
    C --> G
    D --> H
    
    style A fill:#C8E6C9
    style B fill:#C8E6C9
    style C fill:#C8E6C9
    style D fill:#C8E6C9
```

**Performance Targets**:

| Metric | Target | Measurement Method |
|--------|--------|-------------------|
| Edge Processing Rate | 500K edges/min | Micrometer timer metrics |
| End-to-End Latency | < 15 minutes | Job start to completion |
| Database Write Rate | 30K inserts/sec | PostgreSQL COPY throughput |
| CPU Utilization | < 80% average | JMX monitoring |
| Memory Usage | < 8GB heap | JVM metrics |
| Concurrent Groups | 2 domains simultaneously | Semaphore permits |

### 5.2 Reliability Requirements

**Availability**: 99.5% monthly uptime (excluding planned maintenance)

**Failure Tolerance**:
- Automatic retry up to 3 attempts with exponential backoff
- Circuit breaker for cascading failure prevention
- Graceful degradation (skip problematic groups)

**Data Integrity**:
- ACID transactions for database writes
- Advisory locks to prevent concurrent updates
- Idempotent processing (safe reruns)

### 5.3 Scalability Requirements

```mermaid
graph TB
    subgraph "Current Scale"
        A1[2.5K nodes/group]
        A2[577K edges/group]
        A3[2 concurrent domains]
        A4[1 concurrent group/domain]
    end
    
    subgraph "Target Scale - 12 months"
        B1[10K nodes/group]
        B2[5M edges/group]
        B3[5 concurrent domains]
        B4[2 concurrent groups/domain]
    end
    
    subgraph "Ultimate Scale - 24 months"
        C1[50K nodes/group]
        C2[100M edges/group]
        C3[Distributed processing]
        C4[Auto-scaling]
    end
    
    A1 -.->|4x growth| B1
    A2 -.->|8.7x growth| B2
    A3 -.->|2.5x growth| B3
    A4 -.->|2x growth| B4
    
    B1 -.->|5x growth| C1
    B2 -.->|20x growth| C2
    B3 -.->|Architecture change| C3
    B4 -.->|Dynamic| C4
    
    style A1 fill:#E3F2FD
    style B1 fill:#FFF9C4
    style C1 fill:#FFCCBC
```

### 5.4 Security Requirements

**Authentication & Authorization**:
- Service-to-service authentication via mutual TLS (future)
- Database access via connection pooling with encrypted credentials
- Read-only access for monitoring endpoints

**Data Protection**:
- Data at rest: PostgreSQL transparent data encryption
- Data in transit: TLS 1.3 for all network communications
- Sensitive data masking in logs

**Compliance**:
- GDPR compliance for EU data processing
- Data retention policies (90-day match history)
- Audit logging for data access

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



## 11. Security Architecture

### 11.1 Security Layers

```mermaid
graph TB
    subgraph "Network Security"
        N1[VPC Isolation]
        N2[Security Groups]
        N3[NACLs]
        N4[TLS 1.3]
    end
    
    subgraph "Application Security"
        A1[Authentication]
        A2[Authorization]
        A3[Input Validation]
        A4[Secrets Management]
    end
    
    subgraph "Data Security"
        D1[Encryption at Rest]
        D2[Encryption in Transit]
        D3[Data Masking]
        D4[Access Logging]
    end
    
    subgraph "Infrastructure Security"
        I1[OS Hardening]
        I2[Patch Management]
        I3[Vulnerability Scanning]
        I4[SIEM Integration]
    end
    
    N1 --> A1
    A1 --> D1
    D1 --> I1
    
    style N1 fill:#FFCDD2
    style A1 fill:#F8BBD0
    style D1 fill:#E1BEE7
    style I1 fill:#C5CAE9
```

### 11.2 Security Controls

| Control | Implementation | Status |
|---------|----------------|--------|
| **Authentication** | Database credentials in AWS Secrets Manager | Implemented |
| **Authorization** | Role-based database access | Implemented |
| **Encryption at Rest** | PostgreSQL TDE, LMDB file permissions | Implemented |
| **Encryption in Transit** | TLS 1.3 for all connections | 📋 Planned |
| **Audit Logging** | PostgreSQL audit log + application logs | Implemented |
| **Secret Rotation** | Automated credential rotation (90 days) | Planned |
| **Vulnerability Scanning** | Snyk + OWASP Dependency Check | Implemented |
| **Penetration Testing** | Annual third-party assessment | Scheduled |

### 11.3 Threat Model

| Threat | Likelihood | Impact | Mitigation |
|--------|-----------|--------|------------|
| SQL Injection | Low | High | Parameterized queries (JPA) |
| Unauthorized Access | Medium | High | Network isolation, authentication |
| Data Breach | Low | Critical | Encryption, access logging |
| Denial of Service | Medium | Medium | Rate limiting, resource quotas |
| Insider Threat | Low | High | Audit logging, least privilege |
| Supply Chain Attack | Medium | High | Dependency scanning, SBOM |

---

## 12. Scalability & Performance

### 12.1 Scaling Dimensions

```mermaid
quadrantChart
    title Scalability Quadrant Analysis
    x-axis Low Effort --> High Effort
    y-axis Low Impact --> High Impact
    
    quadrant-1 Do First
    quadrant-2 Strategic
    quadrant-3 Consider Later
    quadrant-4 Quick Wins
    
    Vertical Scaling: [0.3, 0.6]
    Horizontal Scaling: [0.7, 0.8]
    Caching Layer: [0.4, 0.7]
    Database Sharding: [0.9, 0.9]
    Async Processing: [0.5, 0.8]
    Connection Pooling: [0.2, 0.5]
    LMDB Optimization: [0.3, 0.7]
    Kafka Integration: [0.8, 0.7]
```

### 12.2 Performance Optimization Strategy

```mermaid
graph LR
    subgraph "Current State"
        A1[577K edges<br/>10 sec]
        A2[2 domains<br/>concurrent]
        A3[25K batch size]
    end
    
    subgraph "Optimization Phase 1"
        B1[Tune GC<br/>G1GC → ZGC]
        B2[Increase permits<br/>2→4 domains]
        B3[Optimize batch<br/>25K→50K]
    end
    
    subgraph "Optimization Phase 2"
        C1[Database<br/>Connection Pool<br/>20→40]
        C2[CPU Threads<br/>4→16]
        C3[COPY Tuning<br/>Binary Protocol]
    end
    
    subgraph "Target State"
        D1[5M edges<br/>< 60 sec]
        D2[4 domains<br/>concurrent]
        D3[100K batch size]
    end
    
    A1 --> B1 --> C1 --> D1
    A2 --> B2 --> C2 --> D2
    A3 --> B3 --> C3 --> D3
    
    style A1 fill:#FFCDD2
    style D1 fill:#C8E6C9
```

### 12.3 Horizontal Scaling Plan

**Phase 1: Active-Passive (Current)**
- Single active instance
- Standby for failover
- Manual switchover

**Phase 2: Active-Active (6 months)**
- Domain-based partitioning
- Each instance handles specific domains
- Coordinated scheduling

**Phase 3: Distributed (12 months)**
- Kafka-based task distribution
- Worker pool architecture
- Auto-scaling based on queue depth

```mermaid
graph TB
    subgraph "Phase 3: Distributed Architecture"
        SCH[Scheduler<br/>Task Producer]
        
        subgraph "Kafka Cluster"
            T1[match-tasks Topic]
            T2[match-results Topic]
        end
        
        subgraph "Worker Pool - Auto-Scaling"
            W1[Worker 1]
            W2[Worker 2]
            W3[Worker N]
        end
        
        SCH -->|Publish Tasks| T1
        T1 -->|Consume| W1
        T1 -->|Consume| W2
        T1 -->|Consume| W3
        
        W1 -->|Publish Results| T2
        W2 -->|Publish Results| T2
        W3 -->|Publish Results| T2
        
        T2 -->|Aggregate| AGG[Results Aggregator]
        AGG --> DB[(PostgreSQL)]
    end
    
    style SCH fill:#4CAF50
    style T1 fill:#FF9800
    style W1 fill:#2196F3
    style DB fill:#9C27B0
```

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

## Appendix C: Decision Log

| Decision | Rationale | Alternatives Considered | Date |
|----------|-----------|------------------------|------|
| Use LMDB for edge storage | Memory-mapped I/O provides 10x read performance vs PostgreSQL | Redis, RocksDB | 2024-01-15 |
| Semaphore-based concurrency | Simple, JVM-native, predictable behavior | Distributed locks (Redis), Database locks | 2024-02-01 |
| PostgreSQL COPY protocol | 10x faster than batch INSERT for bulk writes | JDBC batch inserts, External ETL tool | 2024-02-20 |
| Spring Boot framework | Enterprise ecosystem, production-proven, team familiarity | Quarkus, Micronaut | 2024-01-05 |
| Symmetric/Asymmetric strategies | Covers 90% of business use cases | ML-based scoring (future) | 2024-02-10 |
| Scheduled batch vs real-time | Predictable resource usage, sufficient for SLA | Event-driven real-time (complexity) | 2024-01-20 |




---

