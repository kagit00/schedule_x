package com.shedule.x.processors;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;
import com.shedule.x.service.PotentialMatchService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
public class PotentialMatchesCreationJobExecutor {
    private final PotentialMatchService potentialMatchService;
    private final ExecutorService batchExecutor;
    private final MeterRegistry meterRegistry;

    private final ConcurrentMap<UUID, Semaphore> pageSemaphores = new ConcurrentHashMap<>();
    @Value("${match.batch-overlap:200}") private int batchOverlap;
    @Value("${match.batch-limit:500}") private int batchLimit;
    @Value("${nodes.limit.full.job:1000}") int nodesLimitForFullJob;
    @Value("${match.max-retries:3}") int maxRetries;
    @Value("${match.retry-delay-millis:1000}") long retryDelayMillis;
    @Value("${match.max-concurrent-pages:1}") int maxConcurrentPages;

    public PotentialMatchesCreationJobExecutor(
            PotentialMatchService potentialMatchService,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            MeterRegistry meterRegistry
    ) {
        this.potentialMatchService = potentialMatchService;
        this.batchExecutor = batchExecutor;
        this.meterRegistry = meterRegistry;
    }


    public CompletableFuture<Void> processGroup(UUID groupId, UUID domainId, String cycleId) {
        Semaphore pageSemaphore = pageSemaphores.computeIfAbsent(groupId, g -> new Semaphore(maxConcurrentPages, true));

        return CompletableFuture.runAsync(() -> {
            AtomicBoolean pageAcquired = new AtomicBoolean(false);
            long totalProcessed = 0;
            int zeroNodeCount = 0;

            // Calculate sliding window parameters
            int step = Math.max(1, batchLimit - batchOverlap);
            int maxWindows = (int) Math.ceil((double) nodesLimitForFullJob / step);

            try {
                if (!pageSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    log.warn("Timed out acquiring pageSemaphore for groupId={}, cycleId={}", groupId, cycleId);
                    meterRegistry.counter("semaphore.acquire.timeout", "groupId", groupId.toString(), "cycleId", cycleId).increment();
                    return;
                }
                pageAcquired.set(true);

                for (int window = 0; window < maxWindows; window++) {
                    int startOffset = window * step;
                    log.info("Processing groupId={}, cycleId={}, window={}, startOffset={}, limit={}, overlap={}",
                            groupId, cycleId, window, startOffset, batchLimit, batchOverlap);

                    NodesCount nodesCount = processPageWithRetriesByOffsetSync(groupId, domainId, cycleId, startOffset, batchLimit);
                    if (nodesCount == null) {
                        log.warn("Skipping window={} for groupId={} due to retries exhausted", window, groupId);
                        continue;
                    }

                    int nodeCount = nodesCount.nodeCount();
                    totalProcessed += nodeCount;

                    if (nodeCount == 0) {
                        if (++zeroNodeCount >= 1) {
                            log.info("Early termination: window={} had no nodes (groupId={}, cycleId={})", window, groupId, cycleId);
                            break;
                        }
                    } else {
                        zeroNodeCount = 0;
                    }

                    if (!nodesCount.hasMoreNodes()) {
                        log.info("No more nodes: groupId={}, cycleId={}, window={}", groupId, cycleId, window);
                        break;
                    }
                }

                log.info("Group finished: groupId={}, cycleId={}, totalProcessed={}", groupId, cycleId, totalProcessed);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted: groupId={}, cycleId={}: {}", groupId, cycleId, e.getMessage());

            } catch (Exception e) {
                log.error("Error processing groupId={}, cycleId={}: {}", groupId, cycleId, e.getMessage(), e);
                meterRegistry.counter("group.processing.errors", "groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId).increment();

            } finally {
                if (pageAcquired.get()) {
                    try {
                        pageSemaphore.release();
                        log.debug("Released permit: groupId={}, cycleId={}, available={}", groupId, cycleId, pageSemaphore.availablePermits());
                    } catch (Exception ex) {
                        log.error("Failed to release pageSemaphore for groupId={}: {}", groupId, ex.getMessage(), ex);
                    }
                } else {
                    log.debug("No page permit to release for groupId={}, cycleId={}", groupId, cycleId);
                }

                if (pageSemaphore.availablePermits() == maxConcurrentPages && !pageSemaphore.hasQueuedThreads()) {
                    pageSemaphores.remove(groupId, pageSemaphore);
                }
            }

        }, batchExecutor);
    }

    private NodesCount processPageWithRetriesByOffsetSync(UUID groupId, UUID domainId, String cycleId, int startOffset, int limit) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                MatchingRequest request = MatchingRequest.builder()
                        .groupId(groupId)
                        .domainId(domainId)
                        .processingCycleId(cycleId)
                        .limit(limit)
                        .isRealTime(false)
                        .build();

                NodesCount result = potentialMatchService.matchByGroup(request, startOffset, limit, cycleId)
                        .get(600, TimeUnit.SECONDS);

                log.info("Success: groupId={}, startOffset={}, nodeCount={}", groupId, startOffset, result.nodeCount());
                request.setNumberOfNodes(result.nodeCount());
                return result;

            } catch (Exception e) {
                log.warn("Retry {}/{} failed for groupId={}, startOffset={}, cycleId={}: {}",
                        attempt, maxRetries, groupId, startOffset, cycleId, e.getMessage());
                meterRegistry.counter("match.retry.attempts", "groupId", groupId.toString(), "domainId", domainId.toString(),
                        "cycleId", cycleId, "startOffset", String.valueOf(startOffset)).increment();

                if (attempt == maxRetries) {
                    log.error("Max retries reached: groupId={}, startOffset={}, cycleId={}", groupId, startOffset, cycleId);
                    meterRegistry.counter("match.retries.exhausted", "groupId", groupId.toString(), "cycleId", cycleId,
                            "startOffset", String.valueOf(startOffset)).increment();
                    return null;
                }

                try {
                    Thread.sleep(retryDelayMillis * (1L << (attempt - 1)));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted during backoff sleep for groupId={}, cycleId={}, startOffset={}", groupId, cycleId, startOffset);
                    return null;
                }
            }
        }
        return null;
    }
}
