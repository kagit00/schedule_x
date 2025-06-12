package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "last_run_perfect_matches")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LastRunPerfectMatches {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "group_id", nullable = false)
    private UUID groupId;

    @Column(name = "domain_id", nullable = false)
    private UUID domainId;

    @Column(name = "node_count")
    private long nodeCount;

    @Column(name = "run_date")
    private LocalDateTime runDate;

    @Column(name = "status")
    private String status;
}