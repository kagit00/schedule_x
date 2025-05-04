package com.shedule.x.matcher.strategies;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.exceptions.InternalServerErrorException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.shedule.x.service.GraphBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.concurrent.*;


@Slf4j
@Component
public class AsyncBlossomMatchingStrategy implements AsyncMatchingStrategy {

    private final MatchingStrategy blossomStrategy;
    private final MatchingStrategy fallbackStrategy;
    private final Executor executor;
    private final long timeoutMillis;

    public AsyncBlossomMatchingStrategy(
            BlossomSymmetricMatchingStrategy blossomStrategy,
            WeightedGreedySymmetricMatchingStrategy fallbackStrategy,
            @Qualifier("blossomExecutor") Executor executor,
            @Value("${matching.timeout.blossom}") long timeoutMillis
    ) {
        this.blossomStrategy = blossomStrategy;
        this.fallbackStrategy = fallbackStrategy;
        this.executor = executor;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public CompletableFuture<Map<String, MatchResult>> matchAsync(GraphBuilder.GraphResult graphResult, String groupId, UUID domainId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Starting Blossom matching...");
                return runWithTimeout(() -> blossomStrategy.match(graphResult, groupId, domainId), timeoutMillis);
            } catch (TimeoutException e) {
                log.warn("Blossom strategy timed out. Falling back to Weighted Greedy.");
                return fallbackStrategy.match(graphResult, groupId, domainId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread was interrupted during Blossom matching", e);
                throw new InternalServerErrorException("Matching was interrupted");
            } catch (ExecutionException e) {
                log.error("Matching failed unexpectedly: {}", e.getMessage(), e);
                throw new InternalServerErrorException("Unexpected error during matching");
            }
        }, executor);
    }


    private <T> T runWithTimeout(Callable<T> task, long timeoutMillis) throws TimeoutException, InterruptedException, ExecutionException {
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        Future<T> future = singleThread.submit(task);
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        } finally {
            singleThread.shutdownNow();
        }
    }
}
