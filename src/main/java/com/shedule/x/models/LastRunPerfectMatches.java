package com.shedule.x.models;

import com.shedule.x.dto.enums.JobStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

@Entity
@Table(
        name = "last_run_perfect_matches",
        uniqueConstraints = @UniqueConstraint(columnNames = {"domain_id", "group_id"})
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class LastRunPerfectMatches {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "domain_id", nullable = false, updatable = false)
    private UUID domainId;

    @Column(name = "group_id", nullable = false, updatable = false)
    private UUID groupId;

    @Column(name = "node_count")
    @Builder.Default
    private long nodeCount = 0L;

    @Column(name = "status", length = 20)
    private String status;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "run_date")
    private LocalDateTime runDate;

    public boolean isPending() {
        return JobStatus.PENDING.name().equals(status);
    }

    public boolean isFailed() {
        return JobStatus.FAILED.name().equals(status);
    }

    public boolean isCompleted() {
        return JobStatus.COMPLETED.name().equals(status);
    }
}