package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "last_match_participation")
@IdClass(LastMatchParticipationId.class)
@AllArgsConstructor
@NoArgsConstructor
@Data
public class LastMatchParticipation {
    @Id
    @Column(name = "group_id")
    private String groupId;

    @Id
    @Column(name = "domain_id")
    private UUID domainId;

    @Column(name = "last_run_timestamp", nullable = false)
    private LocalDateTime lastRunTimestamp;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}