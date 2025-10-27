package com.shedule.x.processors;

import com.shedule.x.models.MatchingAlgorithm;
import com.shedule.x.models.MatchingConfiguration;
import com.shedule.x.models.MatchingGroup;
import com.shedule.x.repo.MatchingAlgorithmRepository;
import com.shedule.x.repo.MatchingConfigurationRepository;
import com.shedule.x.repo.MatchingGroupRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
//@Component
@Profile("!prod")
@RequiredArgsConstructor
public class TestDataSeeder2 {

    private final MatchingAlgorithmRepository algorithmRepository;
    private final MatchingGroupRepository groupRepository;
    private final MatchingConfigurationRepository configurationRepository;

    //@PostConstruct
    public void seedTestData() {
        if (configurationRepository.count() > 0) {
            log.info("➡ Matching configurations already exist, skipping seeding.");
            return;
        }

        log.info("🌱 Seeding MatchingConfiguration test data...");
        List<MatchingAlgorithm> algorithms = algorithmRepository.findAll();
        if (algorithms.isEmpty()) {
            log.warn("No matching algorithms found! Please seed algorithms first.");
            return;
        }

        List<MatchingGroup> groups = groupRepository.findAll();
        if (groups.isEmpty()) {
            log.warn("No matching groups found! Please create sample groups first.");
            return;
        }

        MatchingGroup datingGroup = groups.stream()
                .filter(g -> g.getGroupId().equalsIgnoreCase("dating-default"))
                .findFirst()
                .orElse(groups.get(0));

        MatchingGroup marriageGroup = groups.stream()
                .filter(g -> g.getGroupId().equalsIgnoreCase("marriage-default"))
                .findFirst()
                .orElse(groups.get(0));


        // Assign algorithms
        MatchingAlgorithm greedySymmetric = findAlgorithm(algorithms, "greedySymmetricMatchingStrategy");
        MatchingAlgorithm weightedGreedy = findAlgorithm(algorithms, "weightedGreedySymmetricMatchingStrategy");
        MatchingAlgorithm hungarian = findAlgorithm(algorithms, "hungarianMatchingStrategy");
        MatchingAlgorithm topKWeighted = findAlgorithm(algorithms, "topKWeightedGreedyMatchingStrategy");

        // Create configurations
        MatchingConfiguration datingConfig = createConfig(datingGroup, topKWeighted, 10, 100, 1);
        MatchingConfiguration weightedDating = createConfig(marriageGroup, topKWeighted, 10, 100, 2);


        configurationRepository.saveAll(List.of(
                datingConfig, weightedDating
        ));

        log.info("MatchingConfiguration seeding completed successfully.");
    }

    private MatchingConfiguration createConfig(MatchingGroup group, MatchingAlgorithm algorithm,
                                                int min, int max, int priority) {
        MatchingConfiguration config = new MatchingConfiguration();
        config.setGroup(group);
        config.setAlgorithm(algorithm);
        config.setNodeCountMin(min);
        config.setNodeCountMax(max);
        config.setPriority(priority);
        config.setCreatedAt(LocalDateTime.now());
        config.setTimeoutMs(10000);
        return config;
    }

    private MatchingAlgorithm findAlgorithm(List<MatchingAlgorithm> all, String id) {
        return all.stream()
                .filter(a -> a.getId().toString().equalsIgnoreCase(id)
                        || a.getName().equalsIgnoreCase(id))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Algorithm not found: " + id));
    }
}
