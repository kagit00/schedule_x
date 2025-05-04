package com.shedule.x.repo;

import com.shedule.x.models.MatchingConfiguration;
import com.shedule.x.models.MatchingGroup;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface MatchingConfigurationRepository extends JpaRepository<MatchingConfiguration, UUID> {
    List<MatchingConfiguration> findByGroupIdOrderByPriorityAsc(UUID groupId);
    MatchingConfiguration findByGroup(MatchingGroup group);
}