# Node Import System - High-Level Design Document


---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#3-system-architecture)
3. [Functional Architecture](#4-functional-architecture)
4. [Non-Functional Requirements](#5-non-functional-requirements)
5. [Technology Stack](#6-technology-stack)
6. [Data Architecture](#7-data-architecture)
7. [Integration Architecture](#8-integration-architecture)


---

## 1. Executive Summary

### 1.1 System Overview

The **Node Import System** is an event-driven, cloud-native data ingestion platform designed to process high-volume entity imports into the matching engine ecosystem. It consumes import requests from Kafka topics, processes CSV files from object storage or reference lists, and bulk-loads entities into PostgreSQL with comprehensive error handling, status tracking, and observability.


## 3. System Architecture

### 3.1 Architectural Style

**Primary Style:** Event-Driven Modular Monolith
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

