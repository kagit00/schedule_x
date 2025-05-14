package com.shedule.x.service;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.repo.PerfectMatchRepository;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import com.shedule.x.utils.db.BatchUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;


@Service
@RequiredArgsConstructor
@Slf4j
public class MatchStorageServiceImpl implements MatchStorageService {
    private final PerfectMatchRepository perfectMatchRepository;
    private static final int BATCH_SIZE = 1000;


    public void saveMatchResults(Map<String, List<MatchResult>> matches, String groupId, UUID domainId) {
        List<PerfectMatchEntity> allResults = matches.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(match -> PerfectMatchEntity.builder()
                                .compatibilityScore(match.getScore())
                                .matchedAt(DefaultValuesPopulator.getCurrentTimestamp())
                                .referenceId(entry.getKey())
                                .matchedReferenceId(match.getPartnerId())
                                .domainId(domainId)
                                .groupId(groupId)
                                .build()))
                .toList();

        List<List<PerfectMatchEntity>> batches = BatchUtils.partition(allResults, BATCH_SIZE);
        int processed = 0;

        for (List<PerfectMatchEntity> batch : batches) {
            perfectMatchRepository.saveAll(batch);
            perfectMatchRepository.flush();
            processed += batch.size();
            log.debug("Saved batch of {} match results ({} of {})", batch.size(), processed, allResults.size());
        }

        log.info("Completed saving {} match results for group '{}'", allResults.size(), groupId);
    }
}