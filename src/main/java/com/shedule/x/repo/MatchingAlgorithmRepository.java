package com.shedule.x.repo;

import com.shedule.x.models.MatchingAlgorithm;
import com.shedule.x.models.MatchingGroup;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface MatchingAlgorithmRepository extends JpaRepository<MatchingAlgorithm, UUID> {
    MatchingAlgorithm findById(String id);
}
