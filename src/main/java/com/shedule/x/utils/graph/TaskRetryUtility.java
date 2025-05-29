package com.shedule.x.utils.graph;

import com.shedule.x.exceptions.InternalServerErrorException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@UtilityClass
public final class TaskRetryUtility {
    private static final int MAX_RETRIES = 3;

    public static <T> CompletableFuture<T> withRetry(
            Supplier<CompletableFuture<T>> task,
            int attempt,
            ExecutorService ex,
            long retryBackoffMs
    ) {
        return task.get().handleAsync((result, throwable) -> {
            if (throwable == null) {
                return CompletableFuture.completedFuture(result);
            } else {
                if (attempt < MAX_RETRIES) {
                    long backoff = retryBackoffMs * (1L << attempt);
                    log.warn("Retry attempt {} after failure: {}, waiting {}ms", attempt + 1, throwable.getMessage(), backoff);
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        CompletableFuture<T> failed = new CompletableFuture<>();
                        failed.completeExceptionally(e);
                        return failed;
                    }
                    return withRetry(task, attempt + 1, ex, retryBackoffMs);
                } else {
                    CompletableFuture<T> failed = new CompletableFuture<>();
                    failed.completeExceptionally(new InternalServerErrorException("Processing failed after " + MAX_RETRIES + " retries"));
                    return failed;
                }
            }
        }, ex).thenCompose(Function.identity());
    }
}
