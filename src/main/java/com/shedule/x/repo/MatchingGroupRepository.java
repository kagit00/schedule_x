package com.shedule.x.repo;

import com.shedule.x.models.MatchingGroup;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface MatchingGroupRepository extends JpaRepository<MatchingGroup, UUID> {
    List<MatchingGroup> findByDomainId(UUID domainId);
    MatchingGroup findByDomainIdAndId(UUID domainId, UUID groupId);
    @Query("SELECT g.id FROM MatchingGroup g WHERE g.domainId = :domainId")
    List<UUID> findGroupIdsByDomainId(UUID domainId);
    Optional<MatchingGroup> findByDomainIdAndGroupId(UUID domainId, String groupId);
}