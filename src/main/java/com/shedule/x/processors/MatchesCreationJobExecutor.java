package com.shedule.x.processors;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.service.MatchStorageService;
import com.shedule.x.service.MatchingService;
import com.shedule.x.service.MatchingServiceImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class MatchesCreationJobExecutor {

    private final MatchingService matchingService;
    private final MatchStorageService matchStorageService;

    @Async("matchesCreationExecutor")
    public void processGroup(String groupId, UUID domainId) {
        try {
            int page = 0;
            int limit = 10000;
            boolean hasMore = true;
            while (hasMore) {
                MatchingRequest request = MatchingRequest.builder()
                        .groupId(groupId).domainId(domainId).limit(limit)
                        .build();

                Map<String, MatchResult> results = matchingService.matchByGroup(request, page);
                int nodeCount = ((MatchingServiceImpl) matchingService).getLastNodeCount();
                if (!results.isEmpty()) {
                    matchStorageService.saveMatchResults(results, groupId, domainId);
                }

                log.info("Processed page {} for groupId={}, nodes: {}, matches: {}", page, groupId, nodeCount, results.size());
                hasMore = nodeCount == limit;
                page++;
            }
        } catch (Exception e) {
            log.error("Error processing groupId={} domainId={}: {}", groupId, domainId, e.getMessage());
        }
    }
}
