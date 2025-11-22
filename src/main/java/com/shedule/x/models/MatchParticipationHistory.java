package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "match_participation_history")
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class MatchParticipationHistory {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "node_id", nullable = false)
    private UUID nodeId;

    @Column(name = "group_id", nullable = false)
    private UUID groupId;

    @Column(name = "domain_id", nullable = false)
    private UUID domainId;

    private String processingCycleId;

    @Column(name = "participated_at", nullable = false)
    private LocalDateTime participatedAt;
}