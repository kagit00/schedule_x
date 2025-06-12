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

@Slf4j
@Component
public class PerfectMatchCreationJobExecutor {

    private final PerfectMatchService perfectMatchService;
    private final ExecutorService batchExecutor;
    private final MeterRegistry meterRegistry;
    private final int maxRetries;
    private final long retryDelayMillis;
    private final int maxConcurrentGroups;
    private final ConcurrentMap<UUID, Semaphore> groupSemaphores = new ConcurrentHashMap<>();

    public PerfectMatchCreationJobExecutor(
            PerfectMatchService perfectMatchService,
            @Qualifier("matchCreationExecutorService") ExecutorService batchExecutor,
            MeterRegistry meterRegistry,
            @Value("${match.max-retries:3}") int maxRetries,
            @Value("${match.retry-delay-millis:1000}") long retryDelayMillis,
            @Value("${match.max-concurrent-groups:1}") int maxConcurrentGroups
    ) {
        this.perfectMatchService = perfectMatchService;
        this.batchExecutor = batchExecutor;
        this.meterRegistry = meterRegistry;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.maxConcurrentGroups = maxConcurrentGroups;
    }

    public CompletableFuture<Void> processGroup(UUID groupId, UUID domainId, String cycleId) {
        Semaphore groupSemaphore = groupSemaphores.computeIfAbsent(groupId, g -> new Semaphore(maxConcurrentGroups, true));

        return CompletableFuture.runAsync(() -> {
            try {
                if (!groupSemaphore.tryAcquire(60, TimeUnit.SECONDS)) {
                    log.warn("Timeout acquiring semaphore for perfect-match group={}, cycleId={}", groupId, cycleId);
                    meterRegistry.counter("semaphore_acquire_timeout", "groupId", groupId.toString(), "cycleId", cycleId, "matchType", "perfect")
                            .increment();
                    return;
                }

                log.info("Processing perfect-match for group={}, cycleId={}, executorActiveThreads={}",
                        groupId, cycleId, getActiveThreads(batchExecutor));

                processGroupWithRetriesAsync(groupId, domainId, cycleId).join();

                log.info("Completed perfect-match for group={}, cycleId={}", groupId, cycleId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted perfect-match group={}, cycleId={}: {}", groupId, cycleId, e.getMessage());
            } catch (Exception e) {
                log.error("Error processing perfect-match group={}, cycleId={}: {}", groupId, cycleId, e.getMessage());
                meterRegistry.counter("group_processing_errors", "groupId", groupId.toString(), "domainId", domainId.toString(),
                        "cycleId", cycleId, "matchType", "perfect").increment();
            } finally {
                groupSemaphore.release();
                log.debug("Released perfect-match permit for group={}, cycleId={}, available={}",
                        groupId, cycleId, groupSemaphore.availablePermits());

                if (groupSemaphore.availablePermits() == maxConcurrentGroups) {
                    groupSemaphores.remove(groupId, groupSemaphore);
                }
            }
        }, batchExecutor);
    }

    private CompletableFuture<Void> processGroupWithRetriesAsync(UUID groupId, UUID domainId, String cycleId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        processGroupWithRetriesRecursive(groupId, domainId, cycleId, 1, future);
        return future;
    }

    private void processGroupWithRetriesRecursive(
            UUID groupId, UUID domainId, String cycleId, int attempt, CompletableFuture<Void> future) {
        if (attempt > maxRetries) {
            meterRegistry.counter("max_retries_exceeded", "groupId", groupId.toString(),
                    "domainId", domainId.toString(), "cycleId", cycleId, "matchType", "perfect").increment();
            log.error("Max retries ({}) exceeded for perfect-match group={}, cycleId={}",
                    maxRetries, groupId, cycleId);
            future.completeExceptionally(new RuntimeException("Max retries exceeded"));
            return;
        }

        MatchingRequest request = MatchingRequest.builder()
                .groupId(groupId)
                .domainId(domainId)
                .processingCycleId(cycleId)
                .limit(0)
                .isRealTime(false)
                .page(0)
                .build();

        perfectMatchService.processAndSaveMatches(request)
                .whenCompleteAsync((v, throwable) -> {
                    if (throwable != null) {
                        log.warn("Retry {}/{} for perfect-match group={}, cycleId={}: {}",
                                attempt, maxRetries, groupId, cycleId, throwable.getMessage());
                        meterRegistry.counter("retry_attempts_total", "groupId", groupId.toString(),
                                "domainId", domainId.toString(), "cycleId", cycleId,
                                "matchType", "perfect").increment();

                        try {
                            Thread.sleep(retryDelayMillis * (1L << (attempt - 1)));
                            processGroupWithRetriesRecursive(groupId, domainId, cycleId, attempt + 1, future);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            log.error("Interrupted during perfect-match retry delay for group={}, cycleId={}",
                                    groupId, cycleId);
                            future.completeExceptionally(ie);
                        }
                    } else {
                        log.info("Successfully processed perfect-match for group={}, cycleId={}", groupId, cycleId);
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