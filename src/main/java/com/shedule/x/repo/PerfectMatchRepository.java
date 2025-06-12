package com.shedule.x.repo;

import com.shedule.x.models.PerfectMatchEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface PerfectMatchRepository extends JpaRepository<PerfectMatchEntity, UUID> {
    List<PerfectMatchEntity> findByGroupIdAndMatchedAtAfter(UUID groupId, Instant fromHoursAgo);

    @Query("SELECT p FROM PerfectMatchEntity p WHERE p.groupId = :groupId ORDER BY p.matchedAt ASC, p.id ASC")
    Page<PerfectMatchEntity> findByGroupId(UUID groupId, Pageable pageable);
}