package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "matching_configurations")
public class MatchingConfiguration {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "group_id", nullable = false)
    private MatchingGroup group;

    @Column(name = "node_count_min", nullable = false)
    private int nodeCountMin;

    @Column(name = "node_count_max")
    private Integer nodeCountMax;

    @Column(name = "priority", nullable = false)
    private int priority;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "algorithm_id", nullable = false)
    private MatchingAlgorithm algorithm;


    @Column(name = "timeout_ms")
    private Integer timeoutMs;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}