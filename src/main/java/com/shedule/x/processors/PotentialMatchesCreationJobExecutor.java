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

@Slf4j
@Component
public class PotentialMatchesCreationJobExecutor {
    private final PotentialMatchService potentialMatchService;
    private final ExecutorService batchExecutor;
    private final MeterRegistry meterRegistry;
    private final int batchLimit;
    private final int nodesLimitForFullJob;
    private final int maxRetries;
    private final long retryDelayMillis;
    private final int maxConcurrentPages;
    private final ConcurrentMap<UUID, Semaphore> pageSemaphores = new ConcurrentHashMap<>();

    public PotentialMatchesCreationJobExecutor(
            PotentialMatchService potentialMatchService,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            MeterRegistry meterRegistry,
            @Value("${match.batch-limit:500}") int batchLimit,
            @Value("${nodes.limit.full.job:1000}") int nodesLimitForFullJob,
            @Value("${match.max-retries:3}") int maxRetries,
            @Value("${match.retry-delay-millis:1000}") long retryDelayMillis,
            @Value("${match.max-concurrent-pages:1}") int maxConcurrentPages
    ) {
        this.potentialMatchService = potentialMatchService;
        this.batchExecutor = batchExecutor;
        this.meterRegistry = meterRegistry;
        this.batchLimit = batchLimit;
        this.nodesLimitForFullJob = nodesLimitForFullJob;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.maxConcurrentPages = maxConcurrentPages;
    }

    public CompletableFuture<Void> processGroup(UUID groupId, UUID domainId, String cycleId) {
        Semaphore pageSemaphore = pageSemaphores.computeIfAbsent(groupId, g -> new Semaphore(maxConcurrentPages, true));

        return CompletableFuture.runAsync(() -> {
            long totalProcessed = 0;
            int zeroNodeCount = 0;
            int maxPages = (int) Math.ceil((double) nodesLimitForFullJob / batchLimit);

            try {
                if (!pageSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    log.warn("Timed out acquiring pageSemaphore for groupId={}, cycleId={}", groupId, cycleId);
                    meterRegistry.counter("semaphore.acquire.timeout", "groupId", groupId.toString(), "cycleId", cycleId).increment();
                    return;
                }

                for (int page = 0; page < maxPages; page++) {
                    log.info("Processing groupId={}, cycleId={}, page={}, permits left={}, activeThreads={}",
                            groupId, cycleId, page, pageSemaphore.availablePermits(), getActiveThreads(batchExecutor));

                    NodesCount nodesCount = processPageWithRetriesSync(groupId, domainId, cycleId, page, batchLimit);
                    if (nodesCount == null) {
                        log.warn("Skipping page={} for groupId={} due to retries exhausted", page, groupId);
                        continue;
                    }

                    int nodeCount = nodesCount.nodeCount();
                    totalProcessed += nodeCount;

                    if (nodeCount == 0) {
                        if (++zeroNodeCount >= 1) {
                            log.info("Early termination: page={} had no nodes (groupId={}, cycleId={})", page, groupId, cycleId);
                            break;
                        }
                    } else {
                        zeroNodeCount = 0;
                    }

                    if (nodeCount > 0 && nodeCount < batchLimit) {
                        log.warn("Partial page detected: groupId={}, cycleId={}, page={}, nodes={}", groupId, cycleId, page, nodeCount);
                    }

                    if (!nodesCount.hasMoreNodes()) {
                        log.info("No more nodes: groupId={}, cycleId={}, page={}", groupId, cycleId, page);
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
                pageSemaphore.release();
                log.debug("Released permit: groupId={}, cycleId={}, available={}", groupId, cycleId, pageSemaphore.availablePermits());

                if (pageSemaphore.availablePermits() == maxConcurrentPages) {
                    pageSemaphores.remove(groupId, pageSemaphore);
                }
            }

        }, batchExecutor);
    }

    private NodesCount processPageWithRetriesSync(UUID groupId, UUID domainId, String cycleId, int page, int limit) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                MatchingRequest request = MatchingRequest.builder()
                        .groupId(groupId)
                        .domainId(domainId)
                        .processingCycleId(cycleId)
                        .limit(limit)
                        .page(page)
                        .isRealTime(false)
                        .build();

                NodesCount result = potentialMatchService.matchByGroup(request, page, cycleId).get(600, TimeUnit.SECONDS);
                log.info("Success: groupId={}, page={}, nodeCount={}", groupId, page, result.nodeCount());
                request.setNumberOfNodes(result.nodeCount());
                return result;

            } catch (Exception e) {
                log.warn("Retry {}/{} failed for groupId={}, page={}, cycleId={}: {}", attempt, maxRetries, groupId, page, cycleId, e.getMessage());
                meterRegistry.counter("match.retry.attempts", "groupId", groupId.toString(), "domainId", domainId.toString(), "cycleId", cycleId, "page", String.valueOf(page)).increment();

                if (attempt == maxRetries) {
                    log.error("Max retries reached: groupId={}, page={}, cycleId={}", groupId, page, cycleId);
                    meterRegistry.counter("match.retries.exhausted", "groupId", groupId.toString(), "cycleId", cycleId, "page", String.valueOf(page)).increment();
                    return null;
                }

                try {
                    Thread.sleep(retryDelayMillis * (1L << (attempt - 1)));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted during backoff sleep for groupId={}, cycleId={}, page={}", groupId, cycleId, page);
                    return null;
                }
            }
        }
        return null;
    }

    private long getActiveThreads(ExecutorService executor) {
        return (executor instanceof ThreadPoolExecutor)
                ? ((ThreadPoolExecutor) executor).getActiveCount()
                : -1;
    }
}
