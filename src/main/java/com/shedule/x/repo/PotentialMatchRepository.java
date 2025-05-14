package com.shedule.x.repo;

import com.shedule.x.models.PotentialMatchEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface PotentialMatchRepository extends JpaRepository<PotentialMatchEntity, UUID> {
    @Query("SELECT p FROM PotentialMatchEntity p WHERE p.groupId = :groupId ORDER BY p.matchedAt ASC, p.id ASC")
    Page<PotentialMatchEntity> findByGroupId(@Param("groupId") String groupId, Pageable pageable);

    @Modifying
    @Query("INSERT INTO PotentialMatchEntity (groupId, referenceId, matchedReferenceId, compatibilityScore, matchedAt, domainId) " +
            "VALUES (:groupId, :referenceId, :matchedReferenceId, :compatibilityScore, :matchedAt, :domainId) " +
            "ON CONFLICT (groupId, referenceId, matchedReferenceId) DO NOTHING")
    void upsertPotentialMatch(@Param("groupId") String groupId, @Param("referenceId") String referenceId,
                              @Param("matchedReferenceId") String matchedReferenceId, @Param("compatibilityScore") Double compatibilityScore,
                              @Param("matchedAt") LocalDateTime matchedAt, @Param("domainId") UUID domainId);
}