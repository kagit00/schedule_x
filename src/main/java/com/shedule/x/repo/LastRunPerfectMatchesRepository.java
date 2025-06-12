package com.shedule.x.repo;

import com.shedule.x.models.LastRunPerfectMatches;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface LastRunPerfectMatchesRepository extends JpaRepository<LastRunPerfectMatches, UUID> {
    @Query("SELECT l FROM LastRunPerfectMatches l WHERE l.domainId = :domainId AND l.groupId = :groupId")
    Optional<LastRunPerfectMatches> findByDomainIdAndGroupId(@Param("domainId") UUID domainId, @Param("groupId") UUID groupId);
}