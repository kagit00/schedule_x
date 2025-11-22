package com.shedule.x.processors;

import com.google.common.util.concurrent.Uninterruptibles;
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

    @Value("${match.batch-limit:1000}")
    private int batchLimit;

    @Value("${match.max-retries:3}")
    private int maxRetries;

    @Value("${match.retry-delay-millis:1000}")
    private long retryDelayMillis;

    private static final int EMPTY_BATCH_TOLERANCE = 3;

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

    public CompletableFuture<Void> processGroup(UUID groupId, UUID domainId, String cycleId) {
        log.info("Starting Job Execution for groupId={} cycleId={}", groupId, cycleId);
        return processGroupRecursive(groupId, domainId, cycleId, 0);
    }

    private CompletableFuture<Void> processGroupRecursive(UUID groupId, UUID domainId, String cycleId, int emptyStreak) {
        // Stop condition: Too many empty pages
        if (emptyStreak >= EMPTY_BATCH_TOLERANCE) {
            log.info("Finished Job Execution (Streak Limit) | groupId={}", groupId);
            return CompletableFuture.completedFuture(null);
        }

        // 1. Fetch IDs (Orchestrator Role)
        return nodeFetchService.fetchNodeIdsByCursor(groupId, domainId, batchLimit, cycleId)
                .thenComposeAsync(page -> {
                    if (page.ids().isEmpty()) {
                        return processGroupRecursive(groupId, domainId, cycleId, emptyStreak + 1);
                    }

                    // 2. Pass IDs to Service (Business Role)
                    return processPageWithRetries(groupId, domainId, cycleId, page.ids())
                            .thenComposeAsync(result -> {
                                // 3. Persist Cursor on Success
                                if (page.lastCreatedAt() != null && page.lastId() != null) {
                                    nodeFetchService.persistCursor(groupId, domainId,
                                            page.lastCreatedAt().atOffset(ZoneOffset.UTC), page.lastId());
                                }

                                if (!page.hasMore()) {
                                    return CompletableFuture.completedFuture(null);
                                }
                                // 4. Recurse
                                return processGroupRecursive(groupId, domainId, cycleId, 0);
                            }, batchExecutor);
                }, batchExecutor);
    }

    private CompletableFuture<NodesCount> processPageWithRetries(
            UUID groupId, UUID domainId, String cycleId, List<UUID> nodeIds) {

        return processPageAttempt(groupId, domainId, cycleId, nodeIds, 1);
    }

    private CompletableFuture<NodesCount> processPageAttempt(
            UUID groupId, UUID domainId, String cycleId, List<UUID> nodeIds, int attempt) {

        MatchingRequest request = MatchingRequest.builder()
                .groupId(groupId)
                .domainId(domainId)
                .processingCycleId(cycleId)
                .limit(nodeIds.size())
                .isRealTime(false)
                .build();

        // Call the service with the IDs we already fetched
        return potentialMatchService.processNodeBatch(nodeIds, request)
                .thenApply(result -> {
                    log.debug("Batch Success: groupId={}, attempt={}", groupId, attempt);
                    return result;
                })
                .exceptionally(t -> null) // Catch for retry logic below
                .thenCompose(result -> {
                    if (result != null) return CompletableFuture.completedFuture(result);

                    // Retry Logic
                    if (attempt >= maxRetries) {
                        log.error("Max Retries Reached for groupId={}", groupId);
                        meterRegistry.counter("match.job.failed_max_retries").increment();
                        // We return a dummy count to allow the cursor to move forward
                        // (or throw exception if you want to stop the whole job)
                        return CompletableFuture.completedFuture(NodesCount.builder().nodeCount(0).build());
                    }

                    long delay = retryDelayMillis * (1L << (attempt - 1));
                    log.warn("Retrying batch groupId={} in {}ms (Attempt {}/{})", groupId, delay, attempt + 1, maxRetries);

                    return CompletableFuture.runAsync(
                            () -> {}, CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS)
                    ).thenCompose(v -> processPageAttempt(groupId, domainId, cycleId, nodeIds, attempt + 1));
                });
    }
}