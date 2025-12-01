package com.shedule.x.repo;

import com.shedule.x.models.PotentialMatchEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

@Repository
public interface PotentialMatchRepository extends JpaRepository<PotentialMatchEntity, UUID> {

    @Query("SELECT p FROM PotentialMatchEntity p WHERE p.groupId = :groupId ORDER BY p.matchedAt ASC, p.id ASC")
    Page<PotentialMatchEntity> findByGroupId(@Param("groupId") UUID groupId, Pageable pageable);

    @Query("SELECT p FROM PotentialMatchEntity p " +
            "WHERE p.groupId = :groupId AND p.domainId = :domainId " +
            "ORDER BY p.compatibilityScore DESC")
    Stream<PotentialMatchEntity> streamByGroupIdAndDomainId(
            @Param("groupId") String groupId,
            @Param("domainId") UUID domainId,
            Pageable pageable);

    default Stream<PotentialMatchEntity> streamByGroupIdAndDomainId(
            String groupId, UUID domainId, long offset, int limit) {
        return streamByGroupIdAndDomainId(groupId, domainId, PageRequest.of((int) (offset / limit), limit));
    }

    @Query("""
           SELECT DISTINCT p.processingCycleId
           FROM PotentialMatchEntity p
           WHERE p.groupId = :groupId
             AND p.domainId = :domainId
           """)
    List<String> findProcessingCycleIdsByGroupAndDomain(
            @Param("groupId") UUID groupId,
            @Param("domainId") UUID domainId
    );

}