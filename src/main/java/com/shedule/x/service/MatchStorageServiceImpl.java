package com.shedule.x.service;

import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.MatchResult;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.processors.PerfectMatchStorageProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


@Service
@RequiredArgsConstructor
@Slf4j
public class MatchStorageServiceImpl implements MatchStorageService {
    private final PerfectMatchStorageProcessor perfectMatchStorageProcessor;


    @Override
    public CompletableFuture<Void> savePerfectMatchResults(Map<String, List<MatchResult>> results, String groupId, UUID domainId) {
        List<PerfectMatchEntity> perfectMatches = results.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(result -> GraphRequestFactory.convertToPerfectMatch(result, entry.getKey(), groupId, domainId)))
                .collect(Collectors.toList());

        return perfectMatchStorageProcessor.savePerfectMatches(perfectMatches, groupId, domainId);
    }
}