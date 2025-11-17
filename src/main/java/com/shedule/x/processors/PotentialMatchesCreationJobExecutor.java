package com.shedule.x.processors;

import com.google.common.util.concurrent.Uninterruptibles;
import com.shedule.x.dto.CursorPage;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;
import com.shedule.x.service.NodeFetchService;
import com.shedule.x.service.PotentialMatchService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


@Component
@Slf4j
public class PotentialMatchesCreationJobExecutor {

    private final PotentialMatchService potentialMatchService;
    private final NodeFetchService nodeFetchService;
    private final MeterRegistry meterRegistry;

    private final ExecutorService batchExecutor;

    @Value("${match.batch-limit:500}")
    private int batchLimit;

    @Value("${match.max-retries:3}")
    private int maxRetries;

    @Value("${match.retry-delay-millis:1000}")
    private long retryDelayMillis;

    public PotentialMatchesCreationJobExecutor(
            PotentialMatchService potentialMatchService,
            NodeFetchService nodeFetchService,
            MeterRegistry meterRegistry,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor
    ) {
        this.potentialMatchService = potentialMatchService;
        this.nodeFetchService = nodeFetchService;
        this.meterRegistry = meterRegistry;
        this.batchExecutor = batchExecutor;
    }

    private static final int EMPTY_BATCH_TOLERANCE = 3;

    public CompletableFuture<Void> processGroup(UUID groupId, UUID domainId, String cycleId) {
        return CompletableFuture.runAsync(() -> {
            int emptyStreak = 0;

            while (emptyStreak < EMPTY_BATCH_TOLERANCE) {
                CursorPage page = nodeFetchService
                        .fetchNodeIdsByCursor(groupId, domainId, batchLimit, cycleId)
                        .join();

                if (page.ids().isEmpty()) {
                    emptyStreak++;
                    log.info("Empty batch {}/{} for groupId={}, cycleId={}", emptyStreak, EMPTY_BATCH_TOLERANCE, groupId, cycleId);
                    continue;
                }

                emptyStreak = 0;

                NodesCount result = processPageWithRetries(groupId, domainId, cycleId, page.ids());
                if (result == null) {
                    log.warn("Failed to process page for groupId={}, retrying next cursor page", groupId);
                    continue;
                }

                if (page.lastCreatedAt() != null && page.lastId() != null) {
                    nodeFetchService.persistCursor(groupId, domainId, page.lastCreatedAt().atOffset(ZoneOffset.UTC), page.lastId());
                }

                if (!page.hasMore()) {
                    log.info("No more nodes to process for groupId={}, cycleId={}", groupId, cycleId);
                    break;
                }
            }

            log.info("Completed processing groupId={}, cycleId={}", groupId, cycleId);
        }, batchExecutor);
    }

    private NodesCount processPageWithRetries(UUID groupId, UUID domainId, String cycleId, List<UUID> nodeIds) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                MatchingRequest request = MatchingRequest.builder()
                        .groupId(groupId)
                        .domainId(domainId)
                        .processingCycleId(cycleId)
                        .limit(nodeIds.size())
                        .isRealTime(false)
                        .build();

                NodesCount result = potentialMatchService
                        .matchByGroup(request, cycleId)
                        .get(600, TimeUnit.SECONDS);

                log.info("Success: groupId={}, processed {} nodes", groupId, result.nodeCount());
                return result;
            } catch (Exception e) {
                log.warn("Retry {}/{} failed for groupId={}, cycleId={}: {}", attempt, maxRetries, groupId, cycleId, e.toString());
                meterRegistry.counter("match.retry.attempts",
                        "groupId", groupId.toString(),
                        "cycleId", cycleId).increment();

                if (attempt == maxRetries) {
                    log.error("Exhausted retries for groupId={}, cycleId={}", groupId, cycleId);
                    meterRegistry.counter("match.retries.exhausted",
                            "groupId", groupId.toString(), "cycleId", cycleId).increment();
                    return null;
                }

                Uninterruptibles.sleepUninterruptibly(
                        retryDelayMillis * (1L << (attempt - 1)), TimeUnit.MILLISECONDS);
            }
        }
        return null;
    }
}