package com.shedule.x.models;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface MatchParticipationHistoryRepository extends JpaRepository<MatchParticipationHistory, Long> {
    @Query("SELECT mph.nodeId FROM MatchParticipationHistory mph WHERE mph.groupId = :groupId AND mph.domainId = :domainId AND mph.participatedAt >= :since")
    List<String> findNodeIdsByGroupIdAndDomainIdAndParticipatedAtAfter(String groupId, UUID domainId, LocalDateTime since);
}