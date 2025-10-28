# Detailed Entity-Relationship Diagram (ERD) - Graph Matching System

## Complete Entity-Relationship Diagram

```mermaid
erDiagram
  domains {
    UUID id PK "Primary Key"
    VARCHAR name "Unique domain name"
    VARCHAR industry "Industry type (e.g., dating)"
    BOOLEAN is_active "Whether domain is active"
    TIMESTAMP created_at "Domain creation timestamp"
  }

  matching_groups {
    UUID id PK "Primary Key"
    UUID domain_id FK "References domains.id"
    VARCHAR group_id "Group identifier (e.g., dating-default)"
    VARCHAR industry "Industry type"
    BOOLEAN is_cost_based "Whether matching is cost-based"
    BOOLEAN is_symmetric "Whether matching is symmetric"
    TIMESTAMP created_at "Group creation timestamp"
  }

  matching_algorithms {
    VARCHAR id PK "Algorithm identifier"
    VARCHAR name "Algorithm name"
    VARCHAR description "Algorithm description"
  }

  matching_configurations {
    UUID id PK "Primary Key"
    UUID group_id FK "References matching_groups.id"
    INTEGER node_count_min "Minimum nodes for matching"
    INTEGER node_count_max "Maximum nodes for matching"
    VARCHAR algorithm_id FK "References matching_algorithms.id"
    INTEGER priority "Configuration priority"
    INTEGER timeout_ms "Processing timeout in milliseconds"
    BOOLEAN cost_based "Cost-based matching flag"
    BOOLEAN real_time "Real-time processing flag"
    TIMESTAMP created_at "Configuration creation timestamp"
  }

  nodes {
    UUID id PK "Primary Key"
    UUID domain_id FK "References domains.id"
    UUID group_id FK "References matching_groups.id"
    VARCHAR type "Node type"
    VARCHAR reference_id "External reference ID"
    BOOLEAN is_processed "Processing status flag"
    TIMESTAMP created_at "Node creation timestamp"
  }

  node_metadata {
    UUID node_id PK FK "References nodes.id"
    VARCHAR meta_key PK "Metadata key"
    TEXT meta_value "Metadata value"
  }

  edges {
    UUID id PK "Primary Key"
    VARCHAR from_node "Source node reference_id"
    VARCHAR to_node "Target node reference_id"
    DOUBLE weight "Edge weight"
    JSONB metadata "Edge metadata in JSON"
    TIMESTAMP created_at "Edge creation timestamp"
    TIMESTAMP updated_at "Edge last update timestamp"
  }

  nodes_import_jobs {
    UUID id PK "Primary Key"
    UUID domain_id FK "References domains.id"
    VARCHAR group_id "Job group identifier"
    VARCHAR status "Job status"
    INTEGER total_nodes "Total nodes to process"
    INTEGER processed_nodes "Nodes processed so far"
    TEXT error_message "Error message if failed"
    VARCHAR started_at "Start timestamp as string"
    VARCHAR ended_at "End timestamp as string"
    TIMESTAMP completed_at "Completion timestamp"
  }

  perfect_matches {
    UUID id PK "Primary Key"
    UUID domain_id FK "References domains.id"
    UUID group_id FK "References matching_groups.id"
    VARCHAR reference_id "First user reference"
    VARCHAR matched_reference_id "Second user reference"
    DOUBLE compatibility_score "Match compatibility score"
    TIMESTAMP matched_at "Match timestamp"
    VARCHAR processing_cycle_id "Processing cycle identifier"
  }

  potential_matches {
    UUID id PK "Primary Key"
    UUID group_id PK "Partition key part 2, references matching_groups.id"
    UUID domain_id FK "References domains.id"
    VARCHAR reference_id "First user reference"
    VARCHAR matched_reference_id "Second user reference"
    DOUBLE compatibility_score "Potential match score"
    TIMESTAMP matched_at "Candidate match timestamp"
    VARCHAR processing_cycle_id "Processing cycle identifier"
  }

  last_run_perfect_matches {
    UUID id PK "Primary Key"
    VARCHAR group_id "Group identifier"
    UUID domain_id "Domain reference"
    BIGINT node_count "Number of nodes processed"
    TIMESTAMP run_date "Run timestamp"
    VARCHAR status "Run status"
  }

  last_match_participation {
    VARCHAR group_id PK "Part of composite PK"
    UUID domain_id PK "Part of composite PK, references domains.id"
    TIMESTAMP last_run_timestamp "Last participation timestamp"
    TIMESTAMP updated_at "Last update timestamp"
  }

  match_participation_history {
    BIGSERIAL id PK "Auto-incrementing primary key"
    UUID node_id "References nodes.id"
    VARCHAR group_id "Group identifier"
    UUID domain_id "References domains.id"
    TIMESTAMP participated_at "Participation timestamp"
  }

  %% Relationships
  domains ||--o{ matching_groups : "has_many"
  domains ||--o{ nodes : "contains"
  domains ||--o{ nodes_import_jobs : "manages"
  domains ||--o{ perfect_matches : "contains_matches"
  domains ||--o{ potential_matches : "contains_candidates"
  domains ||--o{ last_run_perfect_matches : "tracks_runs"
  domains ||--o{ last_match_participation : "tracks_participation"
  domains ||--o{ match_participation_history : "records_history"

  matching_groups ||--o{ matching_configurations : "configures_with"
  matching_groups ||--o{ nodes : "groups"
  matching_groups ||--o{ perfect_matches : "produces_matches"
  matching_groups ||--o{ potential_matches : "produces_candidates"

  matching_algorithms ||--o{ matching_configurations : "implemented_by"

  nodes ||--o{ node_metadata : "has_metadata"
  nodes ||--o{ edges : "source_of"
  nodes ||--o{ edges : "target_of"
  nodes ||--o{ match_participation_history : "participates_in"

  matching_groups }o--|| domains : "belongs_to_domain"
  nodes }o--|| domains : "belongs_to_domain"
  nodes_import_jobs }o--|| domains : "belongs_to_domain"
  perfect_matches }o--|| domains : "belongs_to_domain"
  potential_matches }o--|| domains : "belongs_to_domain"
  last_run_perfect_matches }o--|| domains : "belongs_to_domain"
  last_match_participation }o--|| domains : "belongs_to_domain"
  match_participation_history }o--|| domains : "belongs_to_domain"

  matching_configurations }o--|| matching_groups : "configured_for_group"
  nodes }o--|| matching_groups : "member_of_group"
  perfect_matches }o--|| matching_groups : "result_of_group"
  potential_matches }o--|| matching_groups : "candidate_from_group"

  matching_configurations }o--|| matching_algorithms : "uses_algorithm"

  node_metadata }o--|| nodes : "describes_node"
  edges }o--|| nodes : "references_source_node"
  edges }o--|| nodes : "references_target_node"
  match_participation_history }o--|| nodes : "records_node_participation"

```

## Database Schema Details

### Core Domain Structure
```sql
-- Domains represent top-level organizational units
CREATE TABLE domains (
    id UUID PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    industry VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Matching groups define logical grouping for matching operations
CREATE TABLE matching_groups (
    id UUID PRIMARY KEY,
    domain_id UUID NOT NULL REFERENCES domains(id),
    group_id VARCHAR(255) NOT NULL,
    industry VARCHAR(50) NOT NULL,
    is_cost_based BOOLEAN NOT NULL,
    is_symmetric BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(domain_id, group_id)
);
```

### Graph Data Model
```sql
-- Nodes represent individual entities in the graph
CREATE TABLE nodes (
    id UUID PRIMARY KEY,
    domain_id UUID NOT NULL REFERENCES domains(id),
    group_id UUID NOT NULL,
    type VARCHAR(50) NOT NULL,
    reference_id VARCHAR(255) NOT NULL,
    is_processed BOOLEAN DEFAULT false NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(reference_id, group_id)
);

-- Edges represent relationships between nodes
CREATE TABLE edges (
    id UUID PRIMARY KEY,
    from_node VARCHAR(255) NOT NULL,
    to_node VARCHAR(255) NOT NULL,
    weight DOUBLE PRECISION DEFAULT 1,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Node metadata stores additional node properties
CREATE TABLE node_metadata (
    node_id UUID NOT NULL REFERENCES nodes(id),
    meta_key VARCHAR(255) NOT NULL,
    meta_value TEXT,
    PRIMARY KEY (node_id, meta_key)
);
```

### Matching Configuration System
```sql
-- Available matching algorithms
CREATE TABLE matching_algorithms (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(255)
);

-- Configuration for matching groups
CREATE TABLE matching_configurations (
    id UUID PRIMARY KEY,
    group_id UUID NOT NULL REFERENCES matching_groups(id),
    node_count_min INTEGER NOT NULL,
    node_count_max INTEGER,
    algorithm_id VARCHAR(100) NOT NULL REFERENCES matching_algorithms(id),
    priority INTEGER NOT NULL,
    timeout_ms INTEGER,
    cost_based BOOLEAN DEFAULT false NOT NULL,
    real_time BOOLEAN DEFAULT false NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(group_id, node_count_min, node_count_max, priority)
);
```

### Match Results Storage
```sql
-- Perfect matches (final matched pairs)
CREATE TABLE perfect_matches (
    id UUID PRIMARY KEY,
    domain_id UUID NOT NULL REFERENCES domains(id),
    group_id UUID NOT NULL,
    reference_id VARCHAR(255) NOT NULL,
    matched_reference_id VARCHAR(255) NOT NULL,
    compatibility_score DOUBLE PRECISION NOT NULL,
    matched_at TIMESTAMP,
    processing_cycle_id VARCHAR(255),
    UNIQUE(group_id, reference_id, matched_reference_id)
);

-- Potential matches (partitioned by group_id for performance)
CREATE TABLE potential_matches (
    id UUID NOT NULL,
    group_id UUID NOT NULL,
    domain_id UUID REFERENCES domains(id),
    reference_id VARCHAR(50) NOT NULL,
    matched_reference_id VARCHAR(50) NOT NULL,
    compatibility_score DOUBLE PRECISION NOT NULL,
    matched_at TIMESTAMP,
    processing_cycle_id VARCHAR(255),
    PRIMARY KEY (id, group_id)
) PARTITION BY LIST (group_id);
```

### Job Management and Tracking
```sql
-- Import/export job tracking
CREATE TABLE nodes_import_jobs (
    id UUID PRIMARY KEY,
    domain_id UUID NOT NULL REFERENCES domains(id),
    group_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    total_nodes INTEGER DEFAULT 0,
    processed_nodes INTEGER DEFAULT 0,
    error_message TEXT,
    started_at VARCHAR(50),
    ended_at VARCHAR(50),
    completed_at TIMESTAMP
);

-- Match participation tracking
CREATE TABLE last_match_participation (
    group_id VARCHAR(255) NOT NULL,
    domain_id UUID NOT NULL REFERENCES domains(id),
    last_run_timestamp TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (group_id, domain_id)
);

-- Historical participation records
CREATE TABLE match_participation_history (
    id BIGSERIAL PRIMARY KEY,
    node_id UUID NOT NULL,
    group_id VARCHAR(255) NOT NULL,
    domain_id UUID NOT NULL REFERENCES domains(id),
    participated_at TIMESTAMP NOT NULL
);
```

## Key Indexes and Performance Optimizations

### Critical Indexes
```sql
-- Nodes table indexes
CREATE INDEX idx_nodes_domain_group ON nodes(domain_id, group_id);
CREATE INDEX idx_nodes_type ON nodes(type);
CREATE INDEX idx_nodes_group_id ON nodes(group_id);
CREATE INDEX idx_nodes_group_type ON nodes(group_id, type);
CREATE INDEX idx_nodes_ref_group ON nodes(reference_id, group_id);

-- Edges table indexes  
CREATE INDEX idx_edges_weight ON edges(weight);
CREATE INDEX idx_edges_from_node ON edges(from_node);
CREATE INDEX idx_edges_to_node ON edges(to_node);

-- Matching tables indexes
CREATE INDEX idx_potential_match_group_id ON potential_matches(group_id);
CREATE INDEX idx_potential_match_reference_id ON potential_matches(reference_id);
CREATE INDEX idx_potential_match_domain_group ON potential_matches(domain_id, group_id);

-- Participation tracking indexes
CREATE INDEX idx_group_domain_timestamp ON match_participation_history(group_id, domain_id, participated_at);
CREATE INDEX idx_node_metadata_node_key ON node_metadata(node_id, meta_key);
```

## Business Logic Flow

1. **Data Ingestion**: Nodes and edges are imported via `nodes_import_jobs`
2. **Configuration**: Matching groups are configured with specific algorithms and parameters
3. **Processing**: Matching algorithms process nodes based on configurations
4. **Results**: Perfect and potential matches are stored in respective tables
5. **Tracking**: Participation and run history are tracked for delta processing
6. **Optimization**: Partitioning and indexing ensure performance at scale

This schema supports a sophisticated graph matching system capable of handling large-scale matching operations with configurable algorithms and comprehensive tracking.