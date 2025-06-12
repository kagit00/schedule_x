package com.shedule.x.models;

import com.shedule.x.dto.enums.JobStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "nodes_import_jobs")
public class NodesImportJob {
    @Id
    @Column(name = "id")
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "domain_id", nullable = false)
    private UUID domainId;

    @Column(name = "group_id", nullable = false)
    private UUID groupId;

    @Column(name = "status", nullable = false)
    private JobStatus status;

    @Column(name = "processed_nodes")
    private int processedNodes;

    @Column(name = "total_nodes")
    private int totalNodes;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "started_at")
    private LocalDateTime startedAt;

    private LocalDateTime endedAt;
    private LocalDateTime completedAt;
}
