package com.shedule.x.models;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@Entity
@Table(name = "perfect_matches", indexes = {
        @Index(name = "idx_perfect_match_group_id", columnList = "groupId"),
        @Index(name = "idx_perfect_match_reference_id", columnList = "referenceId"),
        @Index(name = "idx_perfect_match_matched_reference_id", columnList = "matchedReferenceId"),
        @Index(name = "idx_perfect_match_participants", columnList = "referenceId,matchedReferenceId")
})
public class PerfectMatchEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private UUID groupId;

    @Column(nullable = false, name = "reference_id")
    private String referenceId;

    @Column(nullable = false, name = "matched_reference_id")
    private String matchedReferenceId;

    private LocalDateTime matchedAt;

    @Column(nullable = false)
    private Double compatibilityScore;

    @Column(nullable = false)
    private UUID domainId;

    @Column(name = "processing_cycle_id")
    private String processingCycleId;
}
