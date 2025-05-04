package com.shedule.x.matcher.strategies.decider;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.enums.StrategyKey;
import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.matcher.strategies.*;
import com.shedule.x.models.MatchingConfiguration;
import com.shedule.x.models.MatchingContext;
import com.shedule.x.models.MatchingGroup;
import com.shedule.x.repo.MatchingConfigurationRepository;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.repo.PerfectMatchRepository;
import com.shedule.x.repo.PotentialMatchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;


@Service
@RequiredArgsConstructor
public class MatchingStrategySelector {

    private final Map<String, MatchingStrategy> strategyMap;
    private final MatchingConfigurationRepository configRepository;
    private final MatchingGroupRepository matchingGroupRepository;

    public MatchingStrategy select(MatchingContext ctx, String groupId) {
        MatchingGroup matchingGroup = matchingGroupRepository.findByDomainIdAndGroupId(ctx.getDomainId(), groupId);
        MatchingConfiguration config = configRepository.findByGroup(matchingGroup);

        String algorithmKey = config.getAlgorithm().getId();
        MatchingStrategy strategy = strategyMap.get(algorithmKey);

        if (strategy == null) {
            throw new BadRequestException("No matching strategy found for algorithm: " + algorithmKey);
        }

        return strategy;
    }
}
