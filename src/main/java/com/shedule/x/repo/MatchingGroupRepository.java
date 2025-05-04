package com.shedule.x.repo;

import com.shedule.x.models.MatchingGroup;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface MatchingGroupRepository extends JpaRepository<MatchingGroup, UUID> {
    List<MatchingGroup> findByDomainId(UUID domainId);
    MatchingGroup findByDomainIdAndGroupId(UUID domainId, String groupId);
    @Query("SELECT g.groupId FROM MatchingGroup g WHERE g.domainId = :domainId")
    List<String> findGroupIdsByDomainId(UUID domainId);
}