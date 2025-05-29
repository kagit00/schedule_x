package com.shedule.x.repo;

import com.shedule.x.models.LastMatchParticipation;
import com.shedule.x.models.LastMatchParticipationId;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface LastMatchParticipationRepository extends JpaRepository<LastMatchParticipation, LastMatchParticipationId> {
    Optional<LastMatchParticipation> findByGroupIdAndDomainId(String groupId, UUID domainId);
}