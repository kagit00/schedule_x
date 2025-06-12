package com.shedule.x.processors;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.ExportedFile;
import com.shedule.x.dto.MatchSuggestionsExchange;
import com.shedule.x.dto.MatchTransfer;
import com.shedule.x.models.Domain;
import com.shedule.x.models.PerfectMatchEntity;
import com.shedule.x.models.PotentialMatchEntity;
import com.shedule.x.service.*;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.ResponseMakerUtility;
import com.shedule.x.utils.basic.StringConcatUtil;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
@Slf4j
public class MatchTransferProcessor {

    private static final String MATCH_EXPORT_TOPIC = "matches-suggestions";

    private final PotentialMatchStreamingService potentialMatchStreamingService;
    private final PerfectMatchStreamingService perfectMatchStreamingService;
    private final ExportService exportService;
    private final ScheduleXProducer scheduleXProducer;
    private final MeterRegistry meterRegistry;
    private final Executor matchTransferExecutor;

    @Value("${match.transfer.batch-size:100000}")
    private int batchSize;


    @CircuitBreaker(name = "matchProcessor", fallbackMethod = "processMatchTransferFallback")
    public CompletableFuture<Void> processMatchTransfer(UUID groupId, Domain domain) {
        ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) matchTransferExecutor;
        meterRegistry.gauge("batch_executor_active_threads", executor, ThreadPoolTaskExecutor::getActiveCount);
        meterRegistry.gauge("batch_executor_queue_size", executor, e -> {
            int size = e.getThreadPoolExecutor().getQueue().size();
            if (size > 80) {
                log.warn("Batch executor queue depth high: {}", size);
                meterRegistry.counter("batch_executor_queue_high", "threshold", "80_percent").increment();
            }
            return size;
        });

        Timer.Sample sample = Timer.start(meterRegistry);
        return CompletableFuture.supplyAsync(() -> {
                    AtomicLong recordCount = new AtomicLong(0);
                    BlockingQueue<List<MatchTransfer>> queue = new LinkedBlockingQueue<>(100);
                    AtomicBoolean done = new AtomicBoolean(false);

                    Consumer<List<PotentialMatchEntity>> potentialConsumer = batch -> {
                        List<MatchTransfer> transfers = batch.stream()
                                .map(ResponseMakerUtility::buildMatchTransfer)
                                .collect(Collectors.toList());
                        recordCount.addAndGet(batch.size());
                        meterRegistry.counter("match_export_batch_count", "type", "potential", "groupId", groupId.toString()).increment();
                        try {
                            queue.put(transfers);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted while queuing potential matches", e);
                        }
                    };

                    Consumer<List<PerfectMatchEntity>> perfectConsumer = batch -> {
                        List<MatchTransfer> transfers = batch.stream()
                                .map(ResponseMakerUtility::buildMatchTransfer)
                                .collect(Collectors.toList());
                        recordCount.addAndGet(batch.size());
                        meterRegistry.counter("match_export_batch_count", "type", "perfect", "groupId", groupId.toString()).increment();
                        try {
                            queue.put(transfers);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted while queuing perfect matches", e);
                        }
                    };

                    CompletableFuture<Void> potentialFuture = CompletableFuture.runAsync(() -> {
                        potentialMatchStreamingService.streamAllMatches(groupId, domain.getId(), potentialConsumer, batchSize);
                    }, matchTransferExecutor);

                    CompletableFuture<Void> perfectFuture = CompletableFuture.runAsync(() -> {
                        perfectMatchStreamingService.streamAllMatches(groupId, domain.getId(), perfectConsumer, batchSize);
                    }, matchTransferExecutor);

                    CompletableFuture<Void> bothDone = CompletableFuture.allOf(potentialFuture, perfectFuture);

                    bothDone.whenComplete((v, t) -> done.set(true));

                    Supplier<Stream<MatchTransfer>> matchStreamSupplier = () -> Stream.generate(() -> {
                                try {
                                    return queue.poll(300, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new RuntimeException("Interrupted while dequeuing matches", e);
                                }
                            })
                            .takeWhile(batch -> {
                                return !(done.get() && (batch == null || batch.isEmpty()));
                            })
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .onClose(() -> done.set(true));

                    CompletableFuture.allOf(potentialFuture, perfectFuture)
                            .whenComplete((v, t) -> done.set(true));

                    return exportAndSend(groupId, domain, matchStreamSupplier)
                            .whenComplete((v, throwable) -> {
                                if (throwable == null) {
                                    log.info("Processed {} records for group {}", recordCount.get(), groupId);
                                    meterRegistry.counter("match_export_records", "groupId", groupId.toString()).increment(recordCount.get());
                                }
                            });
                }, matchTransferExecutor)
                .thenCompose(v -> v)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to process matches for group {}", groupId, throwable);
                        meterRegistry.counter("match_process_failed", "groupId", groupId.toString()).increment();
                    }
                    sample.stop(meterRegistry.timer("match_process_duration", "groupId", groupId.toString()));
                })
                .exceptionally(throwable -> {
                    log.error("Failed to process group {}", groupId, throwable);
                    meterRegistry.counter("match_process_failed", "groupId", groupId.toString()).increment();
                    throw new RuntimeException(throwable);
                });
    }

    public CompletableFuture<Void> processMatchTransferFallback(UUID groupId, Domain domain, Throwable t) {
        log.error("Circuit breaker tripped for group {}: {}", groupId, t.getMessage());
        meterRegistry.counter("match_circuit_breaker_tripped", "groupId", groupId.toString()).increment();
        return CompletableFuture.completedFuture(null);
    }

    @Retryable(
            value = {java.net.ConnectException.class, java.util.concurrent.TimeoutException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2)
    )
    public void exportAndSendSync(UUID groupId, Domain domain, Supplier<Stream<MatchTransfer>> matchesSupplier) {
        Timer.Sample sample = Timer.start(meterRegistry);
        ExportedFile file = exportService.exportMatches(matchesSupplier, groupId.toString(), domain.getId()).join();

        MatchSuggestionsExchange payload = GraphRequestFactory.buildFileReference(
                groupId.toString(),
                file.getFilePath(),
                file.getFileName(),
                file.getContentType(),
                domain.getId()
        );

        String topic = StringConcatUtil.concatWithSeparator("-", domain.getName().toLowerCase(), MATCH_EXPORT_TOPIC);
        String key = StringConcatUtil.concatWithSeparator("-", domain.getId().toString(), groupId.toString());
        scheduleXProducer.sendMessage(topic, key, BasicUtility.stringifyObject(payload), false);

        log.info("Exported matches for group '{}' for domain: {}", groupId, domain.getName());
        meterRegistry.counter("match_export_success", "groupId", groupId.toString()).increment();
        sample.stop(meterRegistry.timer("export_send_duration", "groupId", groupId.toString()));
    }

    private CompletableFuture<Void> exportAndSend(UUID groupId, Domain domain, Supplier<Stream<MatchTransfer>> matchesSupplier) {
        return CompletableFuture.supplyAsync(() -> {
            exportAndSendSync(groupId, domain, matchesSupplier);
            return null;
        }, matchTransferExecutor);
    }
}