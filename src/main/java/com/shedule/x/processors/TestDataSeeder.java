package com.shedule.x.processors;

import com.shedule.x.models.MatchingAlgorithm;
import com.shedule.x.models.MatchingConfiguration;
import com.shedule.x.models.MatchingGroup;
import com.shedule.x.repo.MatchingAlgorithmRepository;
import com.shedule.x.repo.MatchingConfigurationRepository;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class TestDataSeeder {
    private final MatchingConfigurationRepository matchingConfigurationRepository;
    private final MatchingGroupRepository matchingGroupRepository;
    private final MatchingAlgorithmRepository matchingAlgorithmRepository;

    @PostConstruct
    @Transactional
    private void save_() {

    }
}
