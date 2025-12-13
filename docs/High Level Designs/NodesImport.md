# Node Import System - High-Level Design Document


---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Context](#2-business-context)
3. [System Architecture](#3-system-architecture)
4. [Functional Architecture](#4-functional-architecture)
5. [Non-Functional Requirements](#5-non-functional-requirements)
6. [Technology Stack](#6-technology-stack)
7. [Data Architecture](#7-data-architecture)
8. [Integration Architecture](#8-integration-architecture)
9. [Deployment Architecture](#9-deployment-architecture)
10. [Security Architecture](#10-security-architecture)
11. [Scalability & Performance](#11-scalability--performance)
12. [Disaster Recovery](#12-disaster-recovery)
13. [Monitoring & Operations](#13-monitoring--operations)
14. [Cost Analysis](#14-cost-analysis)
15. [Migration Strategy](#15-migration-strategy)
16. [Future Roadmap](#16-future-roadmap)

---

## 1. Executive Summary

### 1.1 System Overview

The **Node Import System** is an event-driven, cloud-native data ingestion platform designed to process high-volume entity imports into the matching engine ecosystem. It consumes import requests from Kafka topics, processes CSV files from object storage or reference lists, and bulk-loads entities into PostgreSQL with comprehensive error handling, status tracking, and observability.

### 1.2 Business Value Proposition

```mermaid
mindmap
  root((Business Value))
    Automation
      Zero manual intervention
      Event-driven processing
      Self-healing capabilities
    Scale
      Millions of nodes/day
      Multi-tenant support
      Horizontal scalability
    Reliability
      99.9% success rate
      DLQ error recovery
      Transactional integrity
    Speed
      10K nodes/sec throughput
      <5 min for 100K imports
      Real-time status updates
    Cost Efficiency
      70% reduction vs manual
      Optimized storage (COPY)
      Cloud-native architecture
```

### 1.3 Key Metrics

| Metric | Current | Target (12M) | Strategic Goal (24M) |
|--------|---------|--------------|---------------------|
| **Daily Import Volume** | 500K nodes | 5M nodes | 50M nodes |
| **Processing Throughput** | 5K nodes/sec | 10K nodes/sec | 50K nodes/sec |
| **Job Success Rate** | 98.5% | 99.5% | 99.9% |
| **Average Latency** | 3 min (100K nodes) | <2 min | <1 min |
| **System Availability** | 99% | 99.9% | 99.99% |
| **Cost per Million Nodes** | $50 | $30 | $10 |

### 1.4 Strategic Alignment

```mermaid
graph LR
    A[Business Strategy:<br/>Data-Driven Matching] --> B[Platform Capability:<br/>Scalable Ingestion]
    B --> C[System Feature:<br/>Node Import]
    C --> D[Business Outcome:<br/>User Growth]
    
    E[Market Demand:<br/>Multi-Tenancy] --> F[Technical Innovation:<br/>Event-Driven Architecture]
    F --> C
    
    G[Operational Excellence:<br/>Automation] --> H[System Design:<br/>Kafka Integration]
    H --> C
    
    style A fill:#4CAF50
    style B fill:#2196F3
    style C fill:#FF9800
    style D fill:#9C27B0
```

---

## 2. Business Context

### 2.1 Problem Statement

Organizations managing large-scale matching platforms face critical data ingestion challenges:

**Current Pain Points:**
- **Manual Data Loading**: CSV uploads via UI are slow, error-prone, and not scalable
- **Batch Processing Delays**: Overnight batch jobs create 24-hour latency
- **Error Recovery**: Failed imports require manual intervention and reruns
- **Multi-Tenant Complexity**: Isolated tenant data requires complex routing
- **Visibility Gaps**: No real-time status updates or progress tracking

**Business Impact:**
- Lost revenue from delayed user onboarding (24-hour delay = $100K revenue risk)
- Poor user experience from import failures without feedback
- High operational costs (~$200K annually for manual processing)
- Compliance risks from data handling errors

### 2.2 Solution Approach

```mermaid
graph TB
    subgraph "Problem Domain"
        P1[Manual CSV Uploads]
        P2[Synchronous Processing]
        P3[No Error Recovery]
        P4[Limited Visibility]
    end
    
    subgraph "Solution Strategy"
        S1[Event-Driven Architecture<br/>Kafka Topics]
        S2[Async Processing<br/>Thread Pools]
        S3[DLQ + Retry Logic<br/>Self-Healing]
        S4[Status Tracking<br/>Job State Machine]
    end
    
    subgraph "Business Outcomes"
        O1[70% Cost Reduction]
        O2[99.5% Success Rate]
        O3[<5 min Processing]
        O4[Real-time Status]
    end
    
    P1 --> S1
    P2 --> S2
    P3 --> S3
    P4 --> S4
    
    S1 --> O1
    S2 --> O2
    S3 --> O3
    S4 --> O4
    
    style P1 fill:#FFCDD2
    style S1 fill:#BBDEFB
    style O1 fill:#C8E6C9
```

### 2.3 Use Cases

#### UC-1: Bulk User Import via File
**Actor:** External System (CRM, User Management)  
**Trigger:** User data export available in MinIO  
**Flow:**
1. System publishes NodeExchange message to Kafka (`{domain}-users` topic)
2. Import system downloads GZIP CSV file from MinIO
3. Streams and parses file in 1000-row batches
4. Bulk inserts nodes + metadata using PostgreSQL COPY
5. Publishes job status (COMPLETED/FAILED) to status topic

**Success Criteria:** 100K nodes imported in <5 minutes with >99% success rate

#### UC-2: Reference-Based Import
**Actor:** Application Service  
**Trigger:** List of user IDs to import  
**Flow:**
1. System publishes NodeExchange with referenceIds list
2. Import system creates Node entities from IDs
3. Partitions into batches and saves via JPA
4. Publishes job completion status

**Success Criteria:** 10K references imported in <30 seconds

#### UC-3: Error Recovery via DLQ
**Actor:** Operations Team  
**Trigger:** Failed import messages in DLQ  
**Flow:**
1. Operations reviews DLQ messages in Kafka UI
2. Identifies root cause (format error, schema change)
3. Fixes source data or system configuration
4. Replays DLQ messages to original topic

**Success Criteria:** Failed imports recovered within 1 hour

### 2.4 Business Rules

| Rule ID | Description | Priority | Enforcement |
|---------|-------------|----------|-------------|
| BR-1 | Each node must have unique (groupId, domainId, referenceId) | P0 | Database unique constraint |
| BR-2 | File size limited to 5GB compressed | P0 | Kafka max message size |
| BR-3 | Job status must be published within 1 minute of completion | P1 | Async Kafka producer |
| BR-4 | Failed batches must not block other batches | P1 | Isolated exception handling |
| BR-5 | Metadata keys must be normalized before storage | P2 | HeaderNormalizer utility |
| BR-6 | Import operations must be idempotent | P0 | UPSERT via ON CONFLICT |

---

## 3. System Architecture

### 3.1 Architectural Style

**Primary Style:** Event-Driven Microservice  
**Secondary Patterns:** Pipes-and-Filters, Saga Pattern (for job lifecycle)

```mermaid
C4Context
    title System Context Diagram - Node Import System
    
    Person(producer, "External Systems", "CRM, User Management, Partner APIs")
    Person(analyst, "Data Analysts", "Query imported data for insights")
    Person(ops, "Operations Team", "Monitor imports & troubleshoot")
    
    System_Boundary(import_boundary, "Node Import System") {
        System(importsys, "Node Import Engine", "Processes entity imports via Kafka events")
    }
    
    System_Ext(kafka, "Kafka Cluster", "Message broker for events")
    System_Ext(minio, "MinIO/S3", "Object storage for CSV files")
    SystemDb_Ext(postgres, "PostgreSQL Cluster", "Nodes, metadata, job status")
    System_Ext(monitoring, "Observability Stack", "Prometheus + Grafana + ELK")
    System_Ext(matching, "Matching Engine", "Consumes imported nodes")
    
    Rel(producer, kafka, "Publishes import requests", "Kafka")
    Rel(producer, minio, "Uploads CSV files", "S3 API")
    Rel(kafka, importsys, "Delivers messages", "Consumer Group")
    Rel(importsys, minio, "Downloads files", "S3 API")
    Rel(importsys, postgres, "Bulk inserts", "JDBC + COPY")
    Rel(importsys, kafka, "Publishes job status", "Producer")
    Rel(importsys, monitoring, "Exports metrics/logs", "HTTP")
    Rel(postgres, matching, "Reads nodes", "SQL")
    Rel(postgres, analyst, "Analytics queries", "SQL")
    Rel(ops, monitoring, "Views dashboards", "HTTPS")
    
    UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="1")
```

### 3.2 Logical Architecture

```mermaid
graph TB
    subgraph "Event Layer"
        A1[Kafka Consumer<br/>Topic Pattern Matching]
        A2[Message Router<br/>Payload Type Detection]
        A3[DLQ Handler<br/>Error Routing]
    end
    
    subgraph "Application Layer"
        B1[Import Job Orchestrator<br/>Lifecycle Management]
        B2[File Processing Service<br/>CSV Streaming]
        B3[Batch Processing Service<br/>Reference Handling]
        B4[Status Management Service<br/>State Tracking]
    end
    
    subgraph "Domain Layer"
        C1[Node Factory<br/>Entity Creation]
        C2[Validation Engine<br/>Business Rules]
        C3[Metadata Normalizer<br/>Key Standardization]
    end
    
    subgraph "Infrastructure Layer"
        D1[Storage Processor<br/>PostgreSQL COPY]
        D2[File Resolver<br/>MinIO/Filesystem]
        D3[Metrics Collector<br/>Prometheus Client]
        D4[Transaction Manager<br/>ACID Control]
    end
    
    subgraph "External Systems"
        E1[(PostgreSQL<br/>Primary Data Store)]
        E2[(MinIO<br/>File Storage)]
        E3[Kafka<br/>Event Bus]
        E4[Prometheus<br/>Metrics]
    end
    
    A1 --> B1
    A2 --> B2
    A3 --> B4
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
    
    C1 --> D1
    C2 --> D2
    C3 --> D3
    
    D1 --> E1
    D2 --> E2
    A1 --> E3
    D3 --> E4
    
    style A1 fill:#4CAF50
    style B1 fill:#2196F3
    style C1 fill:#FF9800
    style D1 fill:#9C27B0
    style E1 fill:#607D8B
```

### 3.3 Component Architecture

```mermaid
C4Container
    title Container Diagram - Node Import System

    Container_Boundary(app, "Import Application") {
        Container(consumer, "Kafka Consumer", "Spring Kafka", "Listens to import topics")
        Container(processor, "Payload Processor", "Java Service", "Parses and validates JSON")
        Container(jobsvc, "Import Job Service", "Java Service", "Orchestrates import lifecycle")
        Container(filesvc, "File Import Service", "Java Service", "Processes CSV files")
        Container(batchsvc, "Batch Import Service", "Java Service", "Processes reference lists")
        Container(storage, "Storage Processor", "Java Component", "PostgreSQL bulk operations")
        Container(status, "Status Updater", "Java Component", "Manages job state")
        Container(metrics, "Metrics Exporter", "Java Component", "Exposes Prometheus metrics")
    }

    ContainerDb(postgres, "PostgreSQL", "Relational DB", "Nodes, metadata, jobs")
    ContainerDb(minio, "MinIO", "Object Storage", "CSV files compressed")
    Container(kafka, "Kafka", "Message Broker", "Import events and status")
    Container(monitoring, "Monitoring", "Prometheus and Grafana", "Metrics and alerts")

    Rel(kafka, consumer, "Delivers messages", "Consumer API")
    Rel(consumer, processor, "Routes payloads", "Sync")
    Rel(processor, jobsvc, "Initiates import", "Async")
    Rel(jobsvc, filesvc, "File based import", "Async")
    Rel(jobsvc, batchsvc, "Reference list import", "Async")
    Rel(filesvc, minio, "Downloads file", "S3 API")
    Rel(filesvc, storage, "Batches nodes", "Async")
    Rel(batchsvc, storage, "Batches nodes", "Async")
    Rel(storage, postgres, "COPY and upsert", "JDBC")
    Rel(jobsvc, status, "Update job state", "Sync")
    Rel(status, postgres, "Persist job", "JDBC")
    Rel(status, kafka, "Publish status", "Producer API")
    Rel(metrics, monitoring, "Scraped metrics", "HTTP")

```

---

## 4. Functional Architecture

### 4.1 Core Capabilities

```mermaid
graph TB
    subgraph "Event Processing"
        F1[Kafka Message Consumption]
        F2[Payload Validation & Parsing]
        F3[DLQ Routing]
        F4[Status Publishing]
    end
    
    subgraph "File Handling"
        F5[MinIO File Download]
        F6[GZIP Decompression]
        F7[Streaming CSV Parsing]
        F8[Batch Accumulation]
    end
    
    subgraph "Data Processing"
        F9[Node Entity Creation]
        F10[Metadata Normalization]
        F11[Batch Partitioning]
        F12[Parallel Processing]
    end
    
    subgraph "Storage Operations"
        F13[PostgreSQL COPY Protocol]
        F14[UPSERT Conflict Resolution]
        F15[Metadata Bulk Insert]
        F16[Transaction Management]
    end
    
    subgraph "Job Management"
        F17[Job Lifecycle Tracking]
        F18[Progress Monitoring]
        F19[Error Aggregation]
        F20[Status Notification]
    end
    
    F1 --> F5
    F5 --> F6
    F6 --> F7
    F7 --> F8
    
    F2 --> F9
    F8 --> F9
    F9 --> F10
    F10 --> F11
    F11 --> F12
    
    F12 --> F13
    F13 --> F14
    F14 --> F15
    F15 --> F16
    
    F1 --> F17
    F12 --> F18
    F16 --> F19
    F19 --> F20
    
    F3 -.-> F1
    F4 -.-> F20
    
    style F1 fill:#C8E6C9
    style F7 fill:#BBDEFB
    style F13 fill:#FFF9C4
    style F17 fill:#FFCCBC
```

### 4.2 Processing Pipeline

```mermaid
flowchart LR
    A[Event Trigger] --> B{Message Type?}
    
    B -->|File-Based Import| C[Download from MinIO]
    B -->|Reference-Based Import| D[Create Nodes from IDs]
    
    C --> E[Stream GZIP CSV]
    E --> F[Parse 1000-row Batches]
    
    D --> G[Partition into 1000-node Batches]
    
    F --> H[Convert to Node Entities]
    G --> H
    
    H --> I[Parallel Batch Processing<br/>8 concurrent threads]
    
    I --> J[PostgreSQL COPY to temp_nodes]
    J --> K[UPSERT via INSERT ON CONFLICT]
    K --> L[Bulk Insert Metadata]
    
    L --> M{All Batches Done?}
    M -->|Yes| N[Aggregate Results]
    M -->|More| I
    
    N --> O{Success Rate?}
    O -->|100%| P[Status: COMPLETED]
    O -->|<100%| Q[Status: FAILED]
    
    P --> R[Publish to Kafka<br/>Job Status Topic]
    Q --> R
    
    style A fill:#4CAF50
    style E fill:#2196F3
    style I fill:#FF9800
    style J fill:#9C27B0
    style R fill:#F44336
```

### 4.3 Feature Matrix

| Feature | Priority | Status | Implementation |
|---------|----------|--------|----------------|
| **File-Based Import (CSV)** | P0 | Complete | GZIP streaming + PostgreSQL COPY |
| **Reference-Based Import** | P0 | Complete | List of IDs → Node creation |
| **MinIO Integration** | P0 |  Complete | S3 API for file download |
| **Local Filesystem Support** | P1 |  Complete | Direct file access |
| **DLQ Error Handling** | P0 |  Complete | Auto-routing + manual replay |
| **Job Status Tracking** | P0 |  Complete | State machine + Kafka publishing |
| **Metadata Normalization** | P1 |  Complete | Header standardization |
| **Batch Timeout Management** | P1 |  Complete | Dynamic timeout calculation |
| **Parallel Processing** | P0 |  Complete | 8 concurrent batch workers |
| **Transaction Safety** | P0 |  Complete | ACID via TransactionTemplate |
| **Retry Mechanism** | P1 |  Complete | RetryTemplate with backoff |
| **REST API for Manual Trigger** | P2 |  Planned | HTTP endpoint for on-demand imports |
| **Real-time Progress Updates** | P2 |  Planned | WebSocket or SSE for live status |
| **Schema Validation** | P3 |  Future | JSON Schema for payload validation |

---

## 5. Non-Functional Requirements

### 5.1 Performance Requirements

```mermaid
graph LR
    subgraph "Throughput Targets"
        T1[Kafka Consumption:<br/>500 msg/sec]
        T2[CSV Parsing:<br/>50K rows/sec]
        T3[DB Write:<br/>50K inserts/sec]
    end
    
    subgraph "Latency Targets"
        L1[Message Processing:<br/><100ms]
        L2[Batch Processing:<br/><2s per 1K nodes]
        L3[Job Completion:<br/><5 min for 100K]
    end
    
    subgraph "Scalability Targets"
        S1[Concurrent Jobs:<br/>10 simultaneous]
        S2[File Size:<br/>Up to 5GB]
        S3[Daily Volume:<br/>5M nodes]
    end
    
    subgraph "Efficiency Targets"
        E1[Memory:<br/><4GB heap]
        E2[CPU:<br/><70% avg]
        E3[Network:<br/><100 Mbps]
    end
    
    style T1 fill:#C8E6C9
    style L1 fill:#BBDEFB
    style S1 fill:#FFF9C4
    style E1 fill:#FFCCBC
```

**Performance SLAs**:

| Metric | Target | Measurement | Tolerance |
|--------|--------|-------------|-----------|
| **Import Throughput** | 10K nodes/sec | Timer metrics | ±20% |
| **Job Completion Time** | <5 min for 100K nodes | End-to-end duration | ±30% |
| **Kafka Lag** | <10 seconds | Consumer lag metric | <60 sec max |
| **Success Rate** | >99% | Job status tracking | >95% min |
| **Database Write Rate** | 50K inserts/sec | COPY protocol metrics | ±25% |
| **Memory Footprint** | <4GB heap | JVM metrics | <6GB max |
| **CPU Utilization** | <70% avg | System metrics | <85% max |

### 5.2 Reliability Requirements

**Availability Target:** 99.9% monthly uptime (excluding planned maintenance)

**Failure Tolerance:**

```mermaid
graph TB
    A[Failure Scenario] --> B{Category}
    
    B -->|Transient| C[Kafka Consumer Error]
    B -->|Data| D[CSV Parse Error]
    B -->|Infrastructure| E[Database Unavailable]
    
    C --> F[Retry 3x with backoff<br/>Exponential delay]
    F --> G{Retry Success?}
    G -->|Yes| H[Continue Processing]
    G -->|No| I[Send to DLQ]
    
    D --> J[Skip row, Log error<br/>Add to failed list]
    J --> K[Mark job as FAILED<br/>Publish partial success]
    
    E --> L[Retry 3x with backoff]
    L --> M{DB Restored?}
    M -->|Yes| N[Resume Processing]
    M -->|No| O[Mark job FAILED<br/>Alert operations]
    
    I --> P[Manual Review & Replay]
    K --> Q[Analyze & Reprocess]
    O --> R[Incident Response]
    
    style C fill:#FFF9C4
    style D fill:#FFF9C4
    style E fill:#FFCDD2
    style H fill:#C8E6C9
```

**Data Integrity:**
- ACID transactions for all database writes
- Idempotent operations (UPSERT via ON CONFLICT)
- DLQ for failed messages (no data loss)
- Job status tracking for audit trail

### 5.3 Scalability Requirements

```mermaid
graph LR
    subgraph "Current Scale"
        C1[500K nodes/day<br/>2 Kafka partitions<br/>8 consumer threads]
    end
    
    subgraph "12-Month Target"
        T1[5M nodes/day<br/>8 Kafka partitions<br/>32 consumer threads]
    end
    
    subgraph "24-Month Target"
        T2[50M nodes/day<br/>32 Kafka partitions<br/>Multi-instance deployment]
    end
    
    C1 -->|10x growth| T1
    T1 -->|10x growth| T2
    
    style C1 fill:#E3F2FD
    style T1 fill:#FFF9C4
    style T2 fill:#C8E6C9
```

**Scalability Strategies**:

| Dimension | Current Approach | 12-Month Plan | 24-Month Plan |
|-----------|------------------|---------------|---------------|
| **Vertical** | 8 cores, 16GB RAM | 16 cores, 32GB RAM | 32 cores, 64GB RAM |
| **Horizontal** | Single instance | 3 instances (active-active) | Auto-scaling (5-20 instances) |
| **Kafka** | 2 partitions | 8 partitions | 32 partitions |
| **Database** | Single primary | Primary + 2 read replicas | Sharded by domain |
| **Storage** | Single MinIO | MinIO cluster (3 nodes) | S3 multi-region |

### 5.4 Maintainability Requirements

- **Code Coverage:** ≥80% unit tests, ≥70% integration tests
- **Documentation:** Inline JavaDoc, architectural diagrams, runbooks
- **Logging:** Structured JSON logs with correlation IDs
- **Observability:** Prometheus metrics, Grafana dashboards, distributed tracing (planned)
- **Deployment:** Blue-green deployments, automated rollback
- **Configuration:** Externalized via Spring Boot properties + Kubernetes ConfigMaps

---

## 6. Technology Stack

### 6.1 Technology Landscape

```mermaid
graph TB
    subgraph "Application Tier"
        A1[Java 17 LTS]
        A2[Spring Boot 3.2.x]
        A3[Spring Kafka 3.x]
        A4[Spring Data JPA]
    end
    
    subgraph "Event Tier"
        B1[Apache Kafka 3.x]
        B2[Kafka Connect Future]
        B3[Schema Registry Future]
    end
    
    subgraph "Storage Tier"
        C1[PostgreSQL 15]
        C2[MinIO S3-compatible]
        C3[HikariCP Connection Pool]
    end
    
    subgraph "Observability Tier"
        D1[Micrometer]
        D2[Prometheus]
        D3[Grafana]
        D4[Logback + ELK]
    end
    
    subgraph "Infrastructure"
        E1[Docker Containers]
        E2[Kubernetes Future]
        E3[AWS/GCP Cloud]
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
| **Java 17** | Programming Language | Kotlin, Go | LTS support, team expertise, ecosystem maturity |
| **Spring Boot** | Application Framework | Quarkus, Micronaut | Enterprise adoption, Spring Kafka integration, productivity |
| **Kafka** | Event Streaming | RabbitMQ, AWS SQS | High throughput, durability, ecosystem (Connect, Streams) |
| **PostgreSQL** | Primary Database | MySQL, MongoDB | JSONB for metadata, COPY protocol, ACID compliance |
| **MinIO** | Object Storage | AWS S3, Azure Blob | S3-compatible, on-prem deployment, cost-effective |
| **Spring Kafka** | Kafka Client | Native Kafka Client | Spring integration, error handling, retry support |
| **Micrometer** | Metrics | Dropwizard | Vendor-neutral, Spring Boot native, Prometheus integration |
| **HikariCP** | Connection Pooling | Apache DBCP, C3P0 | Fastest pool, low overhead, production-proven |

### 6.3 Dependency Management

```yaml
Key Dependencies:
  Spring Boot: 3.2.x
    - spring-boot-starter-web
    - spring-boot-starter-data-jpa
    - spring-boot-starter-actuator
  
  Kafka:
    - spring-kafka: 3.x
    - kafka-clients: 3.6.x
  
  Database:
    - postgresql: 42.7.x
    - HikariCP: 5.1.x
  
  File Processing:
    - minio: 8.5.x
    - commons-csv: 1.10.x
  
  Utilities:
    - lombok: 1.18.x
    - jackson-databind: 2.15.x
    - guava: 32.x
  
  Testing:
    - junit-jupiter: 5.10.x
    - mockito-core: 5.x
    - testcontainers: 1.19.x
    - embedded-kafka: 3.x
```

---

## 7. Data Architecture

### 7.1 Conceptual Data Model

```mermaid
erDiagram
    DOMAIN ||--o{ MATCHING_GROUP : contains
    MATCHING_GROUP ||--o{ NODE : contains
    MATCHING_GROUP ||--o{ NODES_IMPORT_JOB : tracks
    NODE ||--o{ NODE_METADATA : has
    NODES_IMPORT_JOB ||--o{ JOB_STATUS_EVENT : publishes
    
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
        boolean processed
        timestamp created_at
        timestamp updated_at
    }
    
    NODE_METADATA {
        bigint id PK
        uuid node_id FK
        string metadata_key
        string metadata_value
        timestamp created_at
    }
    
    NODES_IMPORT_JOB {
        uuid id PK
        uuid group_id FK
        uuid domain_id FK
        string status
        int total_nodes
        int processed_nodes
        text failure_reason
        timestamp created_at
        timestamp updated_at
    }
    
    JOB_STATUS_EVENT {
        uuid id PK
        uuid job_id FK
        string status
        int processed
        int total
        jsonb success_list
        jsonb failed_list
        timestamp published_at
    }
```

### 7.2 Data Flow Architecture

```mermaid
flowchart TB
    subgraph "Data Sources"
        A1[External CRM] -->|Export| A2[MinIO Bucket]
        A3[Partner API] -->|JSON| A4[Kafka Topic]
    end
    
    subgraph "Ingestion Layer"
        A2 -->|S3 API| B1[File Download]
        A4 -->|Kafka Consumer| B2[Message Processing]
    end
    
    subgraph "Processing Layer"
        B1 --> C1[CSV Streaming Parser]
        B2 --> C2[JSON Payload Parser]
        C1 --> C3[Node Entity Factory]
        C2 --> C3
    end
    
    subgraph "Transformation Layer"
        C3 --> D1[Metadata Normalization]
        D1 --> D2[Batch Partitioning]
        D2 --> D3[Parallel Processing]
    end
    
    subgraph "Storage Layer - PostgreSQL"
        D3 --> E1[(temp_nodes<br/>Staging Table)]
        E1 --> E2[(nodes<br/>Primary Table)]
        E2 --> E3[(node_metadata<br/>Attributes)]
        D3 --> E4[(nodes_import_job<br/>Status Tracking)]
    end
    
    subgraph "Event Publishing"
        E4 --> F1[Build Status Message]
        F1 --> F2[Kafka Producer]
        F2 --> F3[Job Status Topic]
    end
    
    subgraph "Data Consumption"
        E2 --> G1[Matching Engine]
        E2 --> G2[Analytics Platform]
        E3 --> G3[Recommendation Service]
    end
    
    style A2 fill:#E3F2FD
    style C1 fill:#FFF9C4
    style E2 fill:#C8E6C9
    style F3 fill:#FFCCBC
```

### 7.3 Data Volumes & Growth

```mermaid
graph LR
    subgraph "Current State"
        CS1[Nodes: 10M<br/>Metadata: 50M<br/>Jobs: 10K<br/>Storage: 50GB]
    end
    
    subgraph "12 Months"
        T1[Nodes: 100M<br/>Metadata: 500M<br/>Jobs: 100K<br/>Storage: 500GB]
    end
    
    subgraph "24 Months"
        T2[Nodes: 1B<br/>Metadata: 5B<br/>Jobs: 1M<br/>Storage: 5TB]
    end
    
    CS1 -->|10x| T1
    T1 -->|10x| T2
    
    style CS1 fill:#E3F2FD
    style T1 fill:#FFF9C4
    style T2 fill:#FFCCBC
```

**Storage Strategy**:

| Data Type | Retention | Archival | Backup |
|-----------|-----------|----------|--------|
| **Nodes** | Indefinite | N/A | Daily full + WAL |
| **Node Metadata** | Indefinite | N/A | Daily full + WAL |
| **Import Jobs** | 90 days active | 2 years cold storage | Daily incremental |
| **CSV Files (MinIO)** | 7 days | Delete after import | None |
| **Kafka Topics** | 7 days | N/A | Not applicable |
| **Metrics** | 30 days | N/A | Not applicable |

### 7.4 Data Quality & Governance

```mermaid
graph TB
    subgraph "Data Quality Rules"
        Q1["Uniqueness<br/>No duplicate group-domain-ref"]
        Q2["Completeness<br/>Required fields non-null"]
        Q3["Validity<br/>ENUM type constraints"]
        Q4["Consistency<br/>Group exists in database"]
    end

    subgraph "Governance Policies"
        G1["Access Control<br/>Row-level security per domain"]
        G2["Data Lineage<br/>Import job tracking"]
        G3["Audit Trail<br/>Job status events"]
        G4["Retention Policy<br/>90 day active jobs"]
    end

    subgraph "Compliance"
        C1["GDPR<br/>Right to erasure"]
        C2["Data Privacy<br/>PII encryption"]
        C3["Multi-Tenancy<br/>Domain isolation"]
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
        U1[CRM System<br/>Salesforce, HubSpot]
        U2[User Management<br/>Auth0, Okta]
        U3[Partner APIs<br/>External Integrations]
        U4[Data Pipelines<br/>Airflow, Fivetran]
    end
    
    subgraph "Node Import System"
        NIS[Core Application]
    end
    
    subgraph "Downstream Systems"
        D1[Matching Engine<br/>Graph Processing]
        D2[Analytics Platform<br/>Snowflake, BigQuery]
        D3[Search Service<br/>Elasticsearch]
        D4[Notification Service<br/>Email, SMS]
    end
    
    subgraph "Infrastructure Services"
        I1[Kafka Cluster<br/>Event Bus]
        I2[MinIO/S3<br/>File Storage]
        I3[PostgreSQL<br/>Data Store]
        I4[Prometheus<br/>Metrics]
    end
    
    U1 -->|Exports CSV| I2
    U2 -->|Publishes Events| I1
    U3 -->|Publishes Events| I1
    U4 -->|Orchestrates| NIS
    
    I1 -->|Consumes| NIS
    I2 -->|Downloads| NIS
    
    NIS -->|Writes| I3
    NIS -->|Publishes Status| I1
    NIS -->|Exports Metrics| I4
    
    I3 -->|Reads| D1
    I3 -->|Syncs| D2
    I1 -->|Consumes Status| D4
    I3 -->|Indexes| D3
    
    style NIS fill:#4CAF50
    style I1 fill:#2196F3
    style I3 fill:#FF9800
```

### 8.2 Integration Patterns

| Integration Point | Pattern | Protocol | Frequency | SLA |
|-------------------|---------|----------|-----------|-----|
| **Kafka → Import System** | Event-Driven Consumer | Kafka Protocol | Real-time | <10 sec lag |
| **MinIO → Import System** | Pull (Download) | S3 API (HTTP) | On-demand | <30 sec download for 1GB |
| **Import System → PostgreSQL** | Database Integration | JDBC + COPY | Batch (1000 rows) | <2 sec per batch |
| **Import System → Kafka (Status)** | Event Publishing | Kafka Protocol | On job completion | <1 min |
| **PostgreSQL → Analytics** | ETL | SQL Query | Hourly | <5 min sync |
| **Import System → Prometheus** | Metrics Push | HTTP | Every 15 sec | <1 min visibility |

### 8.3 API Contracts

#### 8.3.1 Kafka Message Formats

**NodeExchange (Import Request)**:
```json
{
  "domainId": "uuid",
  "groupId": "uuid-or-string",
  "filePath": "http://minio:9000/bucket/file.csv.gz",
  "fileName": "users_2024-12-12.csv.gz",
  "contentType": "application/gzip",
  "referenceIds": ["ref1", "ref2", "..."]  // Optional, for reference-based
}
```

**NodesTransferJobExchange (Status Response)**:
```json
{
  "jobId": "uuid",
  "groupId": "uuid-or-string",
  "domainId": "uuid",
  "status": "COMPLETED | FAILED",
  "processed": 98500,
  "total": 100000,
  "successList": ["ref1", "ref2", "..."],  
  "failedList": ["ref99", "ref100"]
}
```

#### 8.3.2 Database Schemas (Version 1.0)

**Nodes Table**:
```sql
CREATE TABLE nodes (
    id UUID PRIMARY KEY,
    reference_id VARCHAR(255) NOT NULL,
    group_id UUID NOT NULL,
    type VARCHAR(50) NOT NULL,
    domain_id UUID NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    UNIQUE(group_id, domain_id, reference_id)
);
```

**Import Jobs Table**:
```sql
CREATE TABLE nodes_import_job (
    id UUID PRIMARY KEY,
    group_id UUID NOT NULL,
    domain_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,
    total_nodes INT DEFAULT 0,
    processed_nodes INT DEFAULT 0,
    failure_reason TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
```

---

## 9. Deployment Architecture

### 9.1 Deployment Topology

```mermaid
graph TB
    subgraph "Cloud / Data Center"
        subgraph "Availability Zone 1"
            subgraph "Application Tier"
                APP1[Import Service<br/>Instance 1<br/>Active]
            end
            
            subgraph "Kafka Cluster"
                K1[Kafka Broker 1<br/>Leader for partitions 0,2]
            end
            
            subgraph "Data Tier"
                DB1[(PostgreSQL<br/>Primary)]
                M1[(MinIO<br/>Node 1)]
            end
        end
        
        subgraph "Availability Zone 2"
            subgraph "Application Tier"
                APP2[Import Service<br/>Instance 2<br/>Active]
            end
            
            subgraph "Kafka Cluster"
                K2[Kafka Broker 2<br/>Leader for partitions 1,3]
            end
            
            subgraph "Data Tier"
                DB2[(PostgreSQL<br/>Standby)]
                M2[(MinIO<br/>Node 2)]
            end
        end
        
        subgraph "Monitoring Zone"
            PROM[Prometheus]
            GRAF[Grafana]
            ELK[ELK Stack]
        end
    end
    
    APP1 -->|Consume| K1
    APP1 -->|Consume| K2
    APP2 -->|Consume| K1
    APP2 -->|Consume| K2
    
    APP1 -->|Read/Write| DB1
    APP2 -->|Read/Write| DB1
    
    DB1 -->|Streaming Replication| DB2
    
    APP1 -->|Download| M1
    APP2 -->|Download| M2
    M1 -->|Replicate| M2
    
    APP1 -->|Metrics| PROM
    APP2 -->|Metrics| PROM
    PROM --> GRAF
    
    APP1 -->|Logs| ELK
    APP2 -->|Logs| ELK
    
    style APP1 fill:#4CAF50
    style APP2 fill:#4CAF50
    style DB1 fill:#2196F3
    style K1 fill:#FF9800
```

### 9.2 Infrastructure Requirements

#### 9.2.1 Compute Resources

| Environment | Instance Type | CPU | Memory | Storage | Instances |
|-------------|--------------|-----|--------|---------|-----------|
| **Production** | c6i.2xlarge (AWS) | 8 vCPUs | 16GB | 100GB SSD | 2 (active-active) |
| **Staging** | c6i.xlarge | 4 vCPUs | 8GB | 50GB SSD | 1 |
| **Development** | t3.large | 2 vCPUs | 8GB | 20GB SSD | 1 |

#### 9.2.2 Infrastructure Resources

| Component | Specification | Purpose | Estimated Cost (Monthly) |
|-----------|--------------|---------|-------------------------|
| **Kafka Cluster** | 3 brokers (m5.large) | Message broker | $450 |
| **PostgreSQL RDS** | db.r6g.2xlarge | Primary database | $800 |
| **MinIO Cluster** | 3 nodes (m5.xlarge) | Object storage | $600 |
| **Load Balancer** | Application LB | Traffic distribution | $50 |
| **Prometheus** | t3.medium | Metrics storage | $30 |
| **Grafana** | t3.small | Dashboards | $15 |
| **ELK Stack** | 3 nodes (r5.large) | Log aggregation | $900 |

### 9.3 Deployment Diagram

```mermaid
C4Deployment
    title Deployment Diagram - Production Environment
    
    Deployment_Node(cloud, "AWS Cloud", "Cloud Provider") {
        Deployment_Node(vpc, "VPC 10.0.0.0/16", "Isolated Network") {
            Deployment_Node(az1, "us-east-1a") {
                Deployment_Node(app1, "EC2 c6i.2xlarge") {
                    Container(import1, "Import Service", "Java 17")
                }
                
                Deployment_Node(kafka1, "EC2 m5.large") {
                    Container(broker1, "Kafka Broker 1", "Kafka 3.6")
                }
                
                Deployment_Node(db1, "RDS db.r6g.2xlarge") {
                    ContainerDb(pg1, "PostgreSQL 15", "Primary")
                }
                
                Deployment_Node(minio1, "EC2 m5.xlarge") {
                    ContainerDb(store1, "MinIO", "Node 1")
                }
            }
            
            Deployment_Node(az2, "us-east-1b") {
                Deployment_Node(app2, "EC2 c6i.2xlarge") {
                    Container(import2, "Import Service", "Java 17")
                }
                
                Deployment_Node(kafka2, "EC2 m5.large") {
                    Container(broker2, "Kafka Broker 2", "Kafka 3.6")
                }
                
                Deployment_Node(db2, "RDS db.r6g.2xlarge") {
                    ContainerDb(pg2, "PostgreSQL 15", "Standby")
                }
                
                Deployment_Node(minio2, "EC2 m5.xlarge") {
                    ContainerDb(store2, "MinIO", "Node 2")
                }
            }
        }
        
        Deployment_Node(monitoring, "Monitoring Cluster") {
            Container(prom, "Prometheus", "Metrics")
            Container(graf, "Grafana", "Dashboards")
        }
    }
    
    Rel(import1, broker1, "Kafka Protocol", "TCP/9092")
    Rel(import1, pg1, "JDBC", "TCP/5432")
    Rel(import1, store1, "S3 API", "HTTP/9000")
    Rel(pg1, pg2, "Replication", "TCP/5432")
    Rel(import1, prom, "Metrics", "HTTP/9090")
```

### 9.4 Deployment Strategy

```mermaid
graph TB
    A[Code Commit] --> B[CI Pipeline<br/>GitHub Actions]
    B --> C[Build & Test<br/>Maven + JUnit]
    C --> D[Docker Build<br/>Multi-stage]
    D --> E[Push to Registry<br/>ECR/Docker Hub]
    
    E --> F{Environment}
    
    F -->|Dev| G[Auto Deploy]
    F -->|Staging| H[Manual Approval]
    F -->|Production| I[Blue-Green Deploy]
    
    G --> J[Health Check]
    H --> J
    I --> J
    
    J -->|Pass| K[Traffic Switch<br/>Update Consumer Group]
    J -->|Fail| L[Rollback<br/>Previous Version]
    
    K --> M[Smoke Tests<br/>Test Import]
    M -->|Pass| N[Monitor 24h]
    M -->|Fail| L
    
    N --> O[Deployment Complete]
    L --> P[Alert Team]
    
    style A fill:#4CAF50
    style I fill:#FF9800
    style K fill:#2196F3
    style L fill:#F44336
```


## 10. Security Architecture

### 10.1 Security Layers

```mermaid
graph TB
    subgraph "Network Security"
        N1[VPC Isolation<br/>Private Subnets]
        N2[Security Groups<br/>Port Restrictions]
        N3[TLS 1.3<br/>In-Transit Encryption]
    end
    
    subgraph "Application Security"
        A1[Authentication<br/>IAM Roles]
        A2[Authorization<br/>RBAC]
        A3[Input Validation<br/>Schema Validation]
        A4[Secrets Management<br/>AWS Secrets Manager]
    end
    
    subgraph "Data Security"
        D1[Encryption at Rest<br/>AES-256]
        D2[PII Masking<br/>Logs & Metrics]
        D3[Access Logging<br/>Audit Trail]
        D4[Data Isolation<br/>Domain-based RLS]
    end
    
    subgraph "Infrastructure Security"
        I1[OS Hardening<br/>CIS Benchmarks]
        I2[Patch Management<br/>Automated Updates]
        I3[Vulnerability Scanning<br/>Snyk + Trivy]
        I4[SIEM Integration<br/>CloudTrail]
    end
    
    N1 --> A1
    A1 --> D1
    D1 --> I1
    
    style N1 fill:#FFCDD2
    style A1 fill:#F8BBD0
    style D1 fill:#E1BEE7
    style I1 fill:#C5CAE9
```

### 10.2 Security Controls

| Control | Implementation | Status | Priority |
|---------|----------------|--------|----------|
| **Authentication** | IAM roles for EC2 instances | ✅ Implemented | P0 |
| **Authorization** | Database role-based access | ✅ Implemented | P0 |
| **Encryption at Rest** | RDS encryption + MinIO SSE | ✅ Implemented | P0 |
| **Encryption in Transit** | TLS for Kafka + JDBC | 📋 Planned | P1 |
| **Secrets Management** | AWS Secrets Manager | ✅ Implemented | P0 |
| **Audit Logging** | CloudTrail + application logs | ✅ Implemented | P1 |
| **Input Validation** | JSON schema validation | 📋 Planned | P2 |
| **Rate Limiting** | Kafka throttling | ✅ Implemented | P2 |
| **DDoS Protection** | AWS Shield | ✅ Implemented | P1 |
| **Vulnerability Scanning** | Snyk for dependencies | ✅ Implemented | P1 |

### 10.3 Threat Model

| Threat | Likelihood | Impact | Risk Level | Mitigation |
|--------|-----------|--------|------------|------------|
| **Malicious Payload Injection** | Medium | High | High | Input validation, schema enforcement |
| **Unauthorized Data Access** | Low | Critical | High | VPC isolation, IAM, encryption |
| **Data Corruption** | Low | High | Medium | ACID transactions, backups |
| **DLQ Message Tampering** | Low | Medium | Low | Access control, audit logs |
| **Kafka Topic Hijacking** | Low | High | Medium | ACLs, TLS, authentication |
| **SQL Injection** | Low | High | Medium | Parameterized queries (JPA) |
| **File Upload Abuse** | Medium | Medium | Medium | File size limits, virus scanning (planned) |

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
        H1[Add Consumer Instances<br/>2 → 5 instances]
        H2[Increase Kafka Partitions<br/>2 → 8 partitions]
        H3[Database Read Replicas<br/>3 replicas]
    end
    
    subgraph "Data Scaling"
        D1[Database Sharding<br/>By domain_id]
        D2[MinIO Distributed Mode<br/>8-node cluster]
        D3[Kafka Topic Partitioning<br/>32 partitions]
    end
    
    V1 -.->|Current| H1
    H1 -.->|12 months| D1
    
    style V1 fill:#E3F2FD
    style H1 fill:#FFF9C4
    style D1 fill:#C8E6C9
```

### 11.2 Performance Optimization

```mermaid
graph LR
    subgraph "Input Optimization"
        I1[Kafka Batching<br/>100 records/poll]
        I2[LZ4 Compression<br/>3x reduction]
        I3[Consumer Prefetching<br/>5MB buffer]
    end
    
    subgraph "Processing Optimization"
        P1[Streaming CSV<br/>No memory load]
        P2[Parallel Batching<br/>8 threads]
        P3[Batch Size Tuning<br/>1000 nodes]
    end
    
    subgraph "Output Optimization"
        O1[PostgreSQL COPY<br/>10x faster]
        O2[Connection Pooling<br/>20 connections]
        O3[Batch Commits<br/>Reduce roundtrips]
    end
    
    I1 --> P1
    I2 --> P2
    I3 --> P3
    
    P1 --> O1
    P2 --> O2
    P3 --> O3
    
    style I1 fill:#C8E6C9
    style P2 fill:#BBDEFB
    style O1 fill:#FFF9C4
```

### 11.3 Capacity Planning

| Year | Daily Import Volume | Peak Throughput | Infrastructure |
|------|-------------------|-----------------|----------------|
| **Current** | 500K nodes | 5K nodes/sec | 2 instances, 2 partitions |
| **Year 1** | 5M nodes | 10K nodes/sec | 5 instances, 8 partitions |
| **Year 2** | 50M nodes | 50K nodes/sec | Auto-scale 5-20 instances, 32 partitions |
| **Year 3** | 500M nodes | 200K nodes/sec | Multi-region, 128 partitions |

**Cost Projection**:

| Component | Current | Year 1 | Year 2 | Year 3 |
|-----------|---------|--------|--------|--------|
| **Compute** | $300/mo | $750/mo | $2K/mo | $8K/mo |
| **Kafka** | $450/mo | $900/mo | $2.5K/mo | $10K/mo |
| **Database** | $800/mo | $1.5K/mo | $4K/mo | $15K/mo |
| **Storage** | $600/mo | $1K/mo | $3K/mo | $10K/mo |
| **Total** | **$2.15K/mo** | **$4.15K/mo** | **$11.5K/mo** | **$43K/mo** |

---

## 12. Disaster Recovery

### 12.1 Backup Strategy

| Component | Frequency | Retention | RTO | RPO |
|-----------|-----------|-----------|-----|-----|
| **PostgreSQL** | Continuous (WAL) | 30 days | <1 hour | <5 min |
| **Kafka Topics** | N/A (replicated) | 7 days | <15 min | 0 (replicated) |
| **MinIO Files** | Daily snapshot | 7 days | <2 hours | <24 hours |
| **Application Config** | Git-versioned | Indefinite | <5 min | 0 |

### 12.2 Disaster Recovery Scenarios

```mermaid
flowchart TD
    A[Incident Detected] --> B{Severity}
    
    B -->|P0 - Critical| C1[Complete System Down]
    B -->|P1 - High| C2[Database Corruption]
    B -->|P2 - Medium| C3[Kafka Unavailable]
    
    C1 --> D1[Activate DR Region]
    D1 --> D2[Promote Standby DB]
    D2 --> D3[Update DNS]
    D3 --> E1[Resume Operations<br/>RTO: 1 hour]
    
    C2 --> F1[Stop Imports]
    F1 --> F2[Restore from WAL<br/>Point-in-time]
    F2 --> F3[Verify Integrity]
    F3 --> E1
    
    C3 --> G1[Use DLQ Replay<br/>From last offset]
    G1 --> G2[Wait for Kafka Recovery]
    G2 --> G3[Resume Consumption]
    G3 --> E1
    
    E1 --> H[Post-Mortem]
    
    style C1 fill:#FFCDD2
    style E1 fill:#C8E6C9
```

---

## 13. Monitoring & Operations

### 13.1 Observability Stack

```mermaid
graph TB
    subgraph "Application"
        APP[Node Import System]
    end
    
    subgraph "Metrics Pipeline"
        APP -->|Micrometer| M1[Prometheus]
        M1 --> M2[Grafana Dashboards]
        M1 --> M3[AlertManager]
        M3 -->|PagerDuty| M4[On-Call Engineer]
    end
    
    subgraph "Logging Pipeline"
        APP -->|Logback| L1[Filebeat]
        L1 --> L2[Logstash]
        L2 --> L3[Elasticsearch]
        L3 --> L4[Kibana]
    end
    
    subgraph "Health Checks"
        APP -->|/actuator/health| H1[Spring Actuator]
        H1 --> H2[Load Balancer]
    end
    
    style APP fill:#4CAF50
    style M1 fill:#FF9800
    style L3 fill:#2196F3
```

### 13.2 Key Dashboards

**Dashboard 1: Import Job Metrics**

```
┌───────────────────────────────────────────────────────┐
│ Node Import System - Job Metrics                     │
├───────────────────────────────────────────────────────┤
│ Jobs Today:               48                          │
│   - Completed: 45 (93.75%)                           │
│   - Failed: 3 (6.25%)                                │
│   - In Progress: 0                                    │
│                                                       │
│ Throughput:               [Graph]                     │
│   - Current: 5K nodes/sec                            │
│   - Peak: 8K nodes/sec                               │
│                                                       │
│ Success Rate (7d):        98.5%                       │
│ Avg Job Duration:         3.2 min                     │
│                                                       │
│ Recent Failures:                                      │
│   - Job abc123: MinIO timeout                        │
│   - Job def456: CSV parse error                      │
└───────────────────────────────────────────────────────┘
```

**Dashboard 2: Kafka Consumer Health**

```
┌───────────────────────────────────────────────────────┐
│ Kafka Consumer Metrics                                │
├───────────────────────────────────────────────────────┤
│ Consumer Lag:             [Graph]                     │
│   - domain-a-users: 5 messages                       │
│   - domain-b-users: 0 messages                       │
│                                                       │
│ Processing Rate:          450 msg/sec                 │
│ Error Rate:               0.5%                        │
│ DLQ Messages:             2 in last hour              │
│                                                       │
│ Thread Pool:              [Graph]                     │
│   - Active: 6/8                                      │
│   - Queue: 12/100                                    │
└───────────────────────────────────────────────────────┘
```

### 13.3 Alerting Rules

| Alert | Condition | Severity | Action | SLA |
|-------|-----------|----------|--------|-----|
| **High Kafka Lag** | Lag >1000 for 5 min | P1 | Scale consumers | <15 min |
| **Job Failure Rate** | >10% failed over 1 hour | P2 | Investigate errors | <30 min |
| **DLQ Spike** | >10 msg/min | P2 | Review message format | <1 hour |
| **Memory Pressure** | Heap >90% | P1 | Restart instance | <5 min |
| **Database Errors** | >5 errors/min | P0 | Check DB health | <5 min |

---

## 14. Cost Analysis

### 14.1 Total Cost of Ownership

```mermaid
pie title Monthly Infrastructure Cost ($2,845)
    "Kafka Cluster" : 450
    "PostgreSQL RDS" : 800
    "MinIO Storage" : 600
    "EC2 Instances" : 600
    "ELK Stack" : 300
    "Monitoring" : 45
    "Data Transfer" : 50
```

**Cost Projection**:

| Component | Current | Year 1 | Year 2 |
|-----------|---------|--------|--------|
| **Infrastructure** | $2.8K/mo | $5K/mo | $12K/mo |
| **Development** | $20K/mo (1 FTE) | $20K/mo | $30K/mo |
| **Operations** | $5K/mo (0.25 FTE) | $10K/mo | $20K/mo |
| **Total Annual** | **$334K** | **$420K** | **$744K** |

### 14.2 Cost-Benefit Analysis

**Benefits** (Annual):
- Eliminated manual processing: **$200K** (2 FTEs)
- Reduced error recovery: **$50K** (downtime costs)
- Faster onboarding: **$100K** (revenue acceleration)
- **Total Annual Benefit: $350K**

**ROI**: ($350K - $334K) / $334K = **4.8% ROI** (Year 1)

### 14.3 Cost Optimization

| Strategy | Saving Potential | Implementation |
|----------|------------------|----------------|
| **Reserved Instances** | 30% on compute | Low |
| **S3 Lifecycle Policies** | 50% on storage | Low |
| **Auto-scaling** | 25% on compute | Medium |
| **Kafka Compression** | 40% on network | Low (already implemented) |

---

## 15. Migration Strategy

### 15.1 Migration Phases

```mermaid
gantt
    title Node Import System - Implementation Timeline
    dateFormat YYYY-MM-DD
    
    section Phase 1: Foundation
    Kafka Integration             :done, p1-1, 2024-01-01, 30d
    Database Schema Setup         :done, p1-2, 2024-01-15, 20d
    MinIO Configuration           :done, p1-3, 2024-02-01, 15d
    
    section Phase 2: Core Development
    File Processing Engine        :done, p2-1, 2024-02-15, 45d
    Reference Import              :done, p2-2, 2024-03-01, 30d
    Status Tracking               :done, p2-3, 2024-03-15, 25d
    
    section Phase 3: Testing
    Unit Testing                  :done, p3-1, 2024-04-01, 20d
    Integration Testing           :done, p3-2, 2024-04-15, 25d
    Performance Testing           :done, p3-3, 2024-05-01, 15d
    
    section Phase 4: Production
    Staging Deployment            :done, p4-1, 2024-05-15, 10d
    Production Pilot (20%)        :active, p4-2, 2024-05-25, 20d
    Full Rollout (100%)           :p4-3, 2024-06-15, 15d
    
    section Phase 5: Optimization
    Performance Tuning            :p5-1, 2024-07-01, 30d
    Documentation                 :p5-2, 2024-07-01, 20d
```

### 15.2 Rollout Strategy

```mermaid
flowchart LR
    A[Development] -->|Tested| B[Staging]
    B -->|Load Tested| C{Phased Rollout}
    
    C -->|Week 1| D1[Pilot: 20% Traffic<br/>2 Domains]
    C -->|Week 2| D2[40% Traffic<br/>5 Domains]
    C -->|Week 3| D3[100% Traffic<br/>All Domains]
    
    D1 --> E{Metrics OK?}
    E -->|Yes| D2
    E -->|No| F[Rollback]
    
    D2 --> G{Metrics OK?}
    G -->|Yes| D3
    G -->|No| F
    
    D3 --> H[Full Production]
    F --> I[Root Cause Analysis]
    
    style D3 fill:#C8E6C9
    style H fill:#4CAF50
    style F fill:#FFCDD2
```

---

## 16. Future Roadmap

### 16.1 Strategic Roadmap

```mermaid
timeline
    title Node Import System Evolution
    
    Q3 2024 : Production Launch
            : File + Reference Import
            : DLQ Support
            : PostgreSQL COPY
    
    Q4 2024 : Performance Optimization
            : Auto-scaling
            : Schema Validation
            : Enhanced Monitoring
    
    Q1 2025 : Multi-Region Support
            : Active-Active Deployment
            : Cross-region Replication
            : Disaster Recovery
    
    Q2 2025 : Advanced Features
            : Real-time Progress API
            : Delta Import Support
            : Data Quality Checks
    
    Q3 2025 : ML Integration
            : Anomaly Detection
            : Smart Batching
            : Predictive Scaling
```

### 16.2 Feature Roadmap

| Quarter | Feature | Business Value | Complexity |
|---------|---------|----------------|------------|
| **Q3 2024** | REST API for Manual Trigger | On-demand imports | Low |
| **Q4 2024** | Schema Validation (JSON Schema) | Reduce parse errors | Medium |
| **Q1 2025** | Real-time Progress WebSocket | Live status updates | Medium |
| **Q2 2025** | Delta Import Support | Faster incremental loads | High |
| **Q3 2025** | ML-based Anomaly Detection | Auto-detect bad data | High |

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Node** | An entity (user, product, resource) in the matching system |
| **Import Job** | A tracked instance of a bulk node import operation |
| **DLQ** | Dead Letter Queue - Kafka topic for failed messages |
| **COPY Protocol** | PostgreSQL bulk loading mechanism (binary format) |
| **UPSERT** | INSERT with ON CONFLICT DO UPDATE (idempotent insert) |
| **Kafka Lag** | Number of messages not yet consumed by consumer group |
| **MinIO** | S3-compatible object storage system |
| **WAL** | Write-Ahead Log (PostgreSQL transaction log) |

---

## Appendix B: References

**Internal Documentation**:
- Node Import System - Low-Level Design (LLD)
- Kafka Topic Configuration Guide
- PostgreSQL COPY Protocol Best Practices
- Operational Runbooks

**External References**:
- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [MinIO S3 API](https://min.io/docs/minio/linux/developers/java/API.html)
- [Kafka Consumer Tuning](https://kafka.apache.org/documentation/#consumerconfigs)

---

