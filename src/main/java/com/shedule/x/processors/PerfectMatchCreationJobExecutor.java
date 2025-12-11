package com.shedule.x.processors;


import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.service.PerfectMatchService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.UUID;
import java.util.concurrent.*;
import lombok.RequiredArgsConstructor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class PerfectMatchCreationJobExecutor {

    private final PerfectMatchService perfectMatchService;
    private final ExecutorService batchExecutor;
    private final MeterRegistry meterRegistry;

    @Value("${match.max-retries:3}")
    private int maxRetries;

    @Value("${match.retry-delay-millis:1000}")
    private long retryDelayMillis;

    public PerfectMatchCreationJobExecutor(
            PerfectMatchService perfectMatchService,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            MeterRegistry meterRegistry) {
                this.perfectMatchService = perfectMatchService;
                this.batchExecutor = batchExecutor;
                this.meterRegistry = meterRegistry;
    }

    public CompletableFuture<Void> processGroup(UUID groupId, UUID domainId) {
        log.info("Starting perfect-match processing for groupId={}, domainId={}", groupId, domainId);

        return processGroupWithRetriesAsync(groupId, domainId)
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        meterRegistry.counter("group_processing_errors",
                                "groupId", groupId.toString(), "matchType", "perfect").increment();
                    } else {
                        log.info("Successfully completed perfect-match for groupId={}", groupId);
                    }
                });
    }

    private CompletableFuture<Void> processGroupWithRetriesAsync(UUID groupId, UUID domainId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        processGroupWithRetriesRecursive(groupId, domainId, 1, future);
        return future;
    }

    private void processGroupWithRetriesRecursive(
            UUID groupId, UUID domainId, int attempt, CompletableFuture<Void> future) {
        if (attempt > maxRetries) {
            meterRegistry.counter("max_retries_exceeded", "groupId", groupId.toString(),
                    "domainId", domainId.toString(), "matchType", "perfect").increment();
            log.error("Max retries ({}) exceeded for perfect-match group={}",
                    maxRetries, groupId);
            future.completeExceptionally(new RuntimeException("Max retries exceeded"));
            return;
        }

        MatchingRequest request = MatchingRequest.builder()
                .groupId(groupId)
                .domainId(domainId)
                .limit(0)
                .isRealTime(false)
                .page(0)
                .build();

        perfectMatchService.processAndSaveMatches(request)
                .whenCompleteAsync((v, throwable) -> {
                    if (throwable != null) {
                        log.warn("Retry {}/{} for perfect-match group={} {}",
                                attempt, maxRetries, groupId, throwable.getMessage());
                        meterRegistry.counter("retry_attempts_total", "groupId", groupId.toString(),
                                "domainId", domainId.toString(),
                                "matchType", "perfect").increment();

                        try {
                            Thread.sleep(retryDelayMillis * (1L << (attempt - 1)));
                            processGroupWithRetriesRecursive(groupId, domainId, attempt + 1, future);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            log.error("Interrupted during perfect-match retry delay for group={}",
                                    groupId);
                            future.completeExceptionally(ie);
                        }
                    } else {
                        log.info("Successfully processed perfect-match for group={}", groupId);
                        future.complete(null);
                    }
                }, batchExecutor);
    }

    private long getActiveThreads(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getActiveCount();
        }
        return -1;
    }
}