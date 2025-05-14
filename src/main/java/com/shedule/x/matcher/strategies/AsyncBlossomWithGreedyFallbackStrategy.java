package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.repo.PotentialMatchRepository;
import com.shedule.x.service.GraphBuilder;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;


@Slf4j
@Component("blossomMatchingStrategy")
public class AsyncBlossomWithGreedyFallbackStrategy implements MatchingStrategy {
    private final AsyncMatchingStrategy asyncBlossom;
    private final MatchingStrategy fallback;
    private final PotentialMatchRepository potentialMatchRepository;
    private final long timeoutMillis;

    public AsyncBlossomWithGreedyFallbackStrategy(
            AsyncMatchingStrategy asyncBlossom,
            @Qualifier("topKWeightedGreedyMatchingStrategy") MatchingStrategy fallback,
            PotentialMatchRepository potentialMatchRepository,
            @Value("${matching.timeout.blossom:2000}") long timeoutMillis) {
        this.asyncBlossom = asyncBlossom;
        this.fallback = fallback;
        this.potentialMatchRepository = potentialMatchRepository;
        this.timeoutMillis = timeoutMillis;
    }

    public Map<String, List<MatchResult>> match(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId) {
        graphResult.potentialMatches().forEach(match -> potentialMatchRepository.upsertPotentialMatch(
                groupId,
                match.userId1(),
                match.userId2(),
                match.compatibilityScore(),
                DefaultValuesPopulator.getCurrentTimestamp(),
                domainId
        ));

        Map<String, List<MatchResult>> finalMatches;
        try {
            finalMatches = asyncBlossom.matchAsync(graphResult, groupId, domainId)
                    .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                    .join();
            if (finalMatches.isEmpty()) {
                log.info("Blossom produced no matches, attempting fallback strategy");
                finalMatches = fallback.match(graphResult, groupId, domainId);
            }
        } catch (CompletionException e) {
            log.warn("Async Blossom failed or timed out: {}, using fallback strategy", e.getCause().getMessage());
            finalMatches = fallback.match(graphResult, groupId, domainId);
        }

        return finalMatches;
    }

    @Override
    public boolean supports(String mode) {
        return "BlossomWithGreedyFallback".equalsIgnoreCase(mode);
    }
}