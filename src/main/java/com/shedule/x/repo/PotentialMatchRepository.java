package com.shedule.x.repo;

import com.shedule.x.models.PotentialMatchEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface PotentialMatchRepository extends JpaRepository<PotentialMatchEntity, UUID> {
    Page<PotentialMatchEntity> findByGroupId(String groupId, Pageable pageable);
}