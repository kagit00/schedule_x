package com.shedule.x.processors;

import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.NodesCount;
import com.shedule.x.service.MatchingService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
@Component
public class MatchesCreationJobExecutor {
    private final MatchingService matchingService;
    private final ExecutorService batchExecutor;
    private final MeterRegistry meterRegistry;
    private final int batchLimit;
    private final int nodesLimitForFullJob;
    private final int maxRetries;
    private final long retryDelayMillis;
    private final int maxConcurrentPages;
    private final ConcurrentMap<String, Semaphore> pageSemaphores = new ConcurrentHashMap<>();

    public MatchesCreationJobExecutor(
            MatchingService matchingService,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            MeterRegistry meterRegistry,
            @Value("${match.batch-limit:500}") int batchLimit,
            @Value("${nodes.limit.full.job:1000}") int nodesLimitForFullJob,
            @Value("${match.max-retries:3}") int maxRetries,
            @Value("${match.retry-delay-millis:1000}") long retryDelayMillis,
            @Value("${match.max-concurrent-pages:1}") int maxConcurrentPages
    ) {
        this.matchingService = matchingService;
        this.batchExecutor = batchExecutor;
        this.meterRegistry = meterRegistry;
        this.batchLimit = batchLimit;
        this.nodesLimitForFullJob = nodesLimitForFullJob;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.maxConcurrentPages = maxConcurrentPages;
    }

    public CompletableFuture<Void> processGroup(String groupId, UUID domainId, String cycleId) {
        Semaphore pageSemaphore = pageSemaphores.computeIfAbsent(groupId, g -> new Semaphore(maxConcurrentPages, true));

        return CompletableFuture.runAsync(() -> {
            long totalProcessed = 0;
            int zeroNodeCount = 0;
            int maxPages = (int) Math.ceil((double) nodesLimitForFullJob / batchLimit);

            try {
                if (!pageSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    log.warn("Timeout acquiring pageSemaphore for group={}, cycleId={}", groupId, cycleId);
                    meterRegistry.counter("semaphore_acquire_timeout", "groupId", groupId, "cycleId", cycleId).increment();
                    return;
                }

                for (int page = 0; page < maxPages; page++) {
                    log.info("Processing page {} of {} for group={}, cycleId={}, permits left: {}, executorActiveThreads={}",
                            page, maxPages - 1, groupId, cycleId, pageSemaphore.availablePermits(), getActiveThreads(batchExecutor));

                    NodesCount nodesCount = processPageWithRetriesSync(groupId, domainId, cycleId, page, batchLimit);
                    if (nodesCount == null) {
                        log.warn("Skipping page {} for group={}, cycleId={} due to failure", page, groupId, cycleId);
                        continue;
                    }

                    int nodeCount = nodesCount.nodeCount();
                    totalProcessed += nodeCount;

                    if (nodeCount == 0) {
                        if (++zeroNodeCount >= 1) {
                            log.info("Empty page detected—stopping early for group={}, cycleId={}", groupId, cycleId);
                            break;
                        }
                    } else {
                        zeroNodeCount = 0;
                    }

                    log.info("Processed group={}, cycleId={}, page={}, nodes={}, totalProcessed={}", groupId, cycleId, page, nodeCount, totalProcessed);

                    if (nodeCount > 0 && nodeCount < batchLimit) {
                        log.warn("Partial page detected for group={}, cycleId={}, page={}, nodes={}, expected={}", groupId, cycleId, page, nodeCount, batchLimit);
                    }

                    if (!nodesCount.hasMoreNodes()) {
                        log.info("No more nodes for group={}, cycleId={} at page={}", groupId, cycleId, page);
                        break;
                    }
                }

                log.info("Completed group={}, cycleId={}, totalProcessed={}", groupId, cycleId, totalProcessed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted group={}, cycleId={}: {}", groupId, cycleId, e.getMessage());
            } catch (Exception e) {
                log.error("Error processing group={}, cycleId={}: {}", groupId, cycleId, e.getMessage());
                meterRegistry.counter("group_processing_errors", "groupId", groupId, "domainId", domainId.toString(), "cycleId", cycleId).increment();
            } finally {
                pageSemaphore.release();
                log.debug("Released permit for group={}, cycleId={}, available={}", groupId, cycleId, pageSemaphore.availablePermits());

                if (pageSemaphore.availablePermits() == maxConcurrentPages) {
                    pageSemaphores.remove(groupId, pageSemaphore);
                }
            }

        }, batchExecutor);
    }

    private NodesCount processPageWithRetriesSync(String groupId, UUID domainId, String cycleId, int page, int limit) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                MatchingRequest request = MatchingRequest.builder()
                        .groupId(groupId)
                        .domainId(domainId)
                        .processingCycleId(cycleId)
                        .limit(limit)
                        .isRealTime(false)
                        .page(page)
                        .build();

                NodesCount nodesCount = matchingService.matchByGroup(request, page, cycleId).get(600, TimeUnit.SECONDS);
                log.info("Processed page={} for groupId={}, cycleId={}, nodeCount={}", page, groupId, cycleId, nodesCount.nodeCount());
                request.setNumberOfNodes(nodesCount.nodeCount());
                return nodesCount;
            } catch (Exception e) {
                log.warn("Retry {}/{} for group={}, cycleId={}, page={}: {}", attempt, maxRetries, groupId, cycleId, page, e.getMessage());
                meterRegistry.counter("retry_attempts_total", "groupId", groupId, "domainId", domainId.toString(), "cycleId", cycleId, "page", String.valueOf(page)).increment();
                if (attempt == maxRetries) {
                    meterRegistry.counter("max_retries_exceeded", "groupId", groupId, "domainId", domainId.toString(), "cycleId", cycleId, "page", String.valueOf(page)).increment();
                    log.error("Max retries ({}) exceeded for group={}, cycleId={}, page={}", maxRetries, groupId, cycleId, page);
                    return null;
                }
                try {
                    Thread.sleep(retryDelayMillis * (1L << (attempt - 1)));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted during retry delay for group={}, cycleId={}, page={}", groupId, cycleId, page);
                    return null;
                }
            }
        }
        return null;
    }

    private long getActiveThreads(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor) executor).getActiveCount();
        }
        return -1;
    }
}