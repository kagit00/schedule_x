# Node Import System - High-Level Design Document


---

This document describes the architectural design of a node import pipeline developed as part of an independent backend systems project. The focus is on system structure, data flow, and correctness rather than production deployment or operational tuning.

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture](#3-system-architecture)
3. [Functional Architecture](#4-functional-architecture)
4. [Technology Stack](#6-technology-stack)
5. [Data Architecture](#7-data-architecture)

---

## 1. Executive Summary

### 1.1 System Overview

The **Node Import System** is an event-driven data ingestion system designed to process entity imports into the matching engine ecosystem. It consumes import requests from messaging topics, processes CSV files from object storage or reference lists, and bulk-loads entities into PostgreSQL with comprehensive error handling, status tracking, and instrumentation.

---

## 3. System Architecture

### 3.1 Logical Architecture

```mermaid
graph TB
    subgraph Event_Layer
        A1[Kafka Consumer\nTopic Pattern Matching]
        A2[Message Router\nPayload Type Detection]
        A3[DLQ Handler\nError Routing]
    end
    
    subgraph Application_Layer
        B1[Import Job Orchestrator\nLifecycle Management]
        B2[File Processing Service\nCSV Streaming]
        B3[Batch Processing Service\nReference Handling]
        B4[Status Management Service\nState Tracking]
    end
    
    subgraph Domain_Layer
        C1[Node Factory\nEntity Creation]
        C2[Validation Engine\nBusiness Rules]
        C3[Metadata Normalizer\nKey Standardization]
    end
    
    subgraph Infrastructure_Layer
        D1[Storage Processor\nPostgreSQL COPY]
        D2[File Resolver\nObject Storage / Filesystem]
        D3[Metrics Collector\nInstrumentation]
        D4[Transaction Manager\nACID Control]
    end
    
    subgraph External_Systems
        E1[(PostgreSQL\nPrimary Data Store)]
        E2[(Object Storage\nFile Storage)]
        E3[Messaging System\nEvent Bus]
        E4[Instrumentation\nMetrics]
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

---

## 4. Functional Architecture

### 4.1 Core Capabilities

```mermaid
graph TB
    subgraph "Event Processing"
        F1[Message Consumption]
        F2[Payload Validation & Parsing]
        F3[DLQ Routing]
        F4[Status Publishing]
    end
    
    subgraph "File Handling"
        F5[File Download]
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
    
    B -->|File-Based Import| C[Download File]
    B -->|Reference-Based Import| D[Create Nodes from IDs]
    
    C --> E[Stream GZIP CSV]
    E --> F[Parse Configurable Batches]
    
    D --> G[Partition into Configurable Batches]
    
    F --> H[Convert to Node Entities]
    G --> H
    
    H --> I[Parallel Batch Processing<br/>Bounded Concurrency]
    
    I --> J[PostgreSQL COPY to temp_nodes]
    J --> K[UPSERT via INSERT ON CONFLICT]
    K --> L[Bulk Insert Metadata]
    
    L --> M{All Batches Done?}
    M -->|Yes| N[Aggregate Results]
    M -->|More| I
    
    N --> O{Success Rate?}
    O -->|100%| P[Status: COMPLETED]
    O -->|<100%| Q[Status: FAILED]
    
    P --> R[Publish to Messaging System<br/>Job Status Topic]
    Q --> R
    
    style A fill:#4CAF50
    style E fill:#2196F3
    style I fill:#FF9800
    style J fill:#9C27B0
    style R fill:#F44336
```

### 4.3 Feature Matrix

| Feature | Priority | Design Coverage | Implementation Notes |
|---------|----------|-----------------|----------------------|
| **File-Based Import (CSV)** | P0 | Supported | GZIP streaming + PostgreSQL COPY |
| **Reference-Based Import** | P0 | Supported | List of IDs → Node creation |
| **Object Storage Integration** | P0 | Supported | S3-compatible API for file download |
| **Local Filesystem Support** | P1 | Supported | Direct file access |
| **DLQ Error Handling** | P0 | Supported | Auto-routing + manual replay |
| **Job Status Tracking** | P0 | Supported | State machine + messaging publishing |
| **Metadata Normalization** | P1 | Supported | Header standardization |
| **Batch Timeout Management** | P1 | Supported | Dynamic timeout calculation |
| **Parallel Processing** | P0 | Supported | Configurable concurrent batch workers |
| **Transaction Safety** | P0 | Supported | ACID via TransactionTemplate |
| **Retry Mechanism** | P1 | Supported | Retry with backoff |
| **REST API for Manual Trigger** | P2 | Planned | HTTP endpoint for on-demand imports |
| **Real-time Progress Updates** | P2 | Planned | WebSocket or SSE for live status |
| **Schema Validation** | P3 | Planned | JSON Schema for payload validation |

---

## 6. Technology Stack

### 6.1 Technology Landscape

```mermaid
graph TB
    subgraph "Application Tier"
        A1[Java]
        A2[Spring Boot]
        A3[Spring Kafka]
        A4[Spring Data JPA]
    end
    
    subgraph "Event Tier"
        B1[Apache Kafka]
        B2[Kafka Connect (Conceptual)]
        B3[Schema Registry (Conceptual)]
    end
    
    subgraph "Storage Tier"
        C1[PostgreSQL]
        C2[S3-Compatible Object Storage]
        C3[Connection Pooling]
    end
    
    subgraph "Instrumentation (Design Intent)"
        D1[Metrics Framework]
        D2[Structured Logging]
    end
    
    subgraph "Deployment Targets (Conceptual)"
        E1[Docker Containers]
        E2[Container Orchestration (Conceptual)]
        E3[Cloud Platforms (Conceptual)]
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
| **Java** | Programming Language | Kotlin, Go | Long-term support, ecosystem maturity |
| **Spring Boot** | Application Framework | Quarkus, Micronaut | Enterprise integration, productivity |
| **Kafka** | Event Streaming | RabbitMQ, Message Queues | High throughput, durability |
| **PostgreSQL** | Primary Database | MySQL, MongoDB | JSONB support, COPY protocol, ACID compliance |
| **S3-Compatible Storage** | Object Storage | Direct Filesystem | Standardized API, flexibility |
| **Spring Kafka** | Kafka Client | Native Client | Integration, error handling support |
| **Metrics Framework** | Instrumentation | Alternative Libraries | Vendor-neutral design |
| **Connection Pooling** | Database Efficiency | Alternative Pools | Performance and reliability |

### 6.3 Dependency Management

```yaml
Key Dependencies:
  Spring Boot:
    - spring-boot-starter-web
    - spring-boot-starter-data-jpa
    - spring-boot-starter-actuator
  
  Kafka:
    - spring-kafka
  
  Database:
    - postgresql
    - Connection Pooling
  
  File Processing:
    - S3 Client
    - CSV Library
  
  Utilities:
    - lombok
    - jackson-databind
    - guava
  
  Testing:
    - junit-jupiter
    - mockito-core
    - testcontainers
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
        A1[External Systems] -->|Export| A2[Object Storage]
        A3[Partner Systems] -->|Payload| A4[Messaging Topic]
    end
    
    subgraph "Ingestion Layer"
        A2 -->|API| B1[File Download]
        A4 -->|Consumer| B2[Message Processing]
    end
    
    subgraph "Processing Layer"
        B1 --> C1[CSV Streaming Parser]
        B2 --> C2[Payload Parser]
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
        F1 --> F2[Producer]
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

### 7.3 Data Quality & Governance

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
        G4["Retention Policy<br/>Active job history"]
    end

    subgraph "Compliance"
        C1["GDPR<br/>Right to erasure"]
        C2["Data Privacy<br/>PII handling"]
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

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Node** | An entity (user, product, resource) in the matching system |
| **Import Job** | A tracked instance of a bulk node import operation |
| **DLQ** | Dead Letter Queue - topic for failed messages |
| **COPY Protocol** | PostgreSQL bulk loading mechanism (binary format) |
| **UPSERT** | INSERT with ON CONFLICT DO UPDATE (idempotent insert) |
| **MinIO** | S3-compatible object storage system |
| **WAL** | Write-Ahead Log (PostgreSQL transaction log) |

---

## Appendix B: References

**Design Documentation**:
- Node Import System - Low-Level Design (LLD)
- Messaging Topic Configuration Guide
- PostgreSQL COPY Protocol Best Practices

**External References**:
- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [PostgreSQL COPY Documentation](https://www.postgresql.org/docs/current/sql-copy.html)
- [S3 API Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)