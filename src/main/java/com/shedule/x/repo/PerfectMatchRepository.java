package com.shedule.x.repo;

import com.shedule.x.models.PerfectMatchEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface PerfectMatchRepository extends JpaRepository<PerfectMatchEntity, UUID> {
    List<PerfectMatchEntity> findByGroupIdAndMatchedAtAfter(String groupId, Instant fromHoursAgo);
    List<PerfectMatchEntity> findByGroupId(String groupId);

    Page<PerfectMatchEntity> findByGroupId(String groupId, Pageable pageable);
}