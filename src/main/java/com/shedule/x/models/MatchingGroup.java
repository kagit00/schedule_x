package com.shedule.x.models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Entity
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "matching_groups")
public class MatchingGroup {
    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "domain_id", nullable = false)
    private UUID domainId;

    @Column(name = "group_id", nullable = false)
    private String groupId;

    @Column(name = "industry", nullable = false)
    private String industry;

    @Column(name = "is_cost_based", nullable = false)
    private boolean isCostBased;

    @Column(name = "is_symmetric", nullable = false)
    private boolean isSymmetric;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}