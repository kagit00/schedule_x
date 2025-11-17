package com.shedule.x.service;

import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.config.factory.ResponseFactory;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.dto.NodeResponse;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.MatchingGroup;
import com.shedule.x.models.Node;
import com.shedule.x.processors.NodesImportProcessor;
import com.shedule.x.processors.NodesImportStatusUpdater;
import com.shedule.x.utils.basic.Constant;
import com.shedule.x.utils.media.csv.CsvParser;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import java.io.InputStream;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class NodesImportService {

    @Value("${import.node-timeout-per-node-ms:50}")
    private long timeoutPerNodeMs;

    @Value("${import.min-batch-timeout-ms:1000}")
    private long minBatchTimeoutMs;

    @Value("${import.max-batch-timeout-ms:30000}")
    private long maxBatchTimeoutMs;

    @Value("${import.max-parallel-futures:8}")
    private int maxParallelFutures;

    @Value("${import.join-timeout-seconds:30}")
    private long joinTimeoutSeconds;

    private final GroupConfigService groupConfigService;
    private final NodesImportStatusUpdater statusUpdater;
    private final NodesImportProcessor nodesImportProcessor;
    private final ResponseFactory<NodeResponse> nodeResponseFactory;
    private final MeterRegistry meterRegistry;
    private final ThreadPoolTaskExecutor executor;

    public NodesImportService(
            NodesImportStatusUpdater statusUpdater,
            NodesImportProcessor nodesImportProcessor,
            ResponseFactory<NodeResponse> nodeResponseFactory,
            MeterRegistry meterRegistry,
            GroupConfigService groupConfigService,
            @Qualifier("nodesImportExecutor") ThreadPoolTaskExecutor executor) {
        this.statusUpdater = statusUpdater;
        this.nodesImportProcessor = nodesImportProcessor;
        this.nodeResponseFactory = nodeResponseFactory;
        this.meterRegistry = meterRegistry;
        this.groupConfigService = groupConfigService;
        this.executor = executor;
    }

    public CompletableFuture<Void> processNodesImport(UUID jobId, MultipartFile file, NodeExchange message) {
        UUID domainId = message.getDomainId();
        MatchingGroup group = groupConfigService.getGroupConfig(message.getGroupId(), domainId);

        return CompletableFuture
                .supplyAsync(() -> {
                    logImportStart(jobId, group.getId().toString(), domainId, file);
                    statusUpdater.updateJobStatus(jobId, JobStatus.PROCESSING);

                    long startTime = System.nanoTime();
                    AtomicInteger totalParsed = new AtomicInteger(0);
                    List<String> success = Collections.synchronizedList(new ArrayList<>());
                    List<String> failed = Collections.synchronizedList(new ArrayList<>());

                    try (InputStream gzipStream = new GZIPInputStream(file.getInputStream())) {
                        processBatchesFromStream(jobId, message, gzipStream, totalParsed, success, failed, startTime);
                    } catch (IOException e) {
                        handleImportFailure(jobId, domainId, group.getGroupId(), e);
                        return null;
                    }

                    finalizeJob(jobId, group.getGroupId(), domainId, success, failed, totalParsed.get(), startTime);
                    return null;
                }, executor)
                .handle((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to process nodes import for jobId={}", jobId, throwable);
                        statusUpdater.handleUnexpectedFailure(jobId, domainId, group.getGroupId(), throwable);
                    }
                    return null;
                });
    }

    private void processBatchesFromStream(UUID jobId, NodeExchange message, InputStream gzipStream,
                                          AtomicInteger totalParsed, List<String> success, List<String> failed, long startTime) {
        List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());
        logExecutorState(jobId);

        CsvParser.parseInBatches(gzipStream, nodeResponseFactory, batch -> {
            CompletableFuture<Void> future = processBatchAsync(jobId, batch, message, success, failed, totalParsed, startTime);
            futures.add(future);

            if (futures.size() >= maxParallelFutures) {
                futures.removeIf(CompletableFuture::isDone);
            }
        });

        joinAndClearFutures(jobId, futures);
    }


    private CompletableFuture<Void> processBatchAsync(UUID jobId, List<NodeResponse> batch, NodeExchange message,
                                                      List<String> success, List<String> failed,
                                                      AtomicInteger totalParsed, long startTime) {
        long batchTimeoutMs = calculateBatchTimeout(batch.size());

        return CompletableFuture
                .runAsync(() -> {
                    nodesImportProcessor.processBatch(jobId, batch, message, success, failed, totalParsed);
                    long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                    log.info("Job {}: Completed batch of {} nodes in {} ms", jobId, batch.size(), durationMs);
                }, executor)
                .orTimeout(batchTimeoutMs, TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> {
                    handleBatchFailure(jobId, batch, failed, throwable, batchTimeoutMs);
                    return null;
                });
    }

    private long calculateBatchTimeout(int batchSize) {
        long calculated = Math.max(minBatchTimeoutMs, batchSize * timeoutPerNodeMs);
        return Math.min(calculated, maxBatchTimeoutMs);
    }

    private void handleBatchFailure(UUID jobId, List<NodeResponse> batch, List<String> failed,
                                    Throwable throwable, long timeoutMs) {
        if (throwable instanceof TimeoutException) {
            log.warn("Job {}: Batch timed out after {}ms (size={})", jobId, timeoutMs, batch.size());
            meterRegistry.counter("node_import_batch_timeout", "jobId", jobId.toString()).increment();
        } else {
            log.error("Job {}: Batch processing failed", jobId, throwable);
            meterRegistry.counter("node_import_batch_error", "jobId", jobId.toString()).increment();
        }
        batch.forEach(node -> failed.add(node.getReferenceId()));
    }


    private void joinAndClearFutures(UUID jobId, List<CompletableFuture<Void>> futures) {
        if (futures.isEmpty()) return;

        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        try {
            allDone.get(joinTimeoutSeconds, TimeUnit.SECONDS);
            futures.clear();
            log.debug("Job {}: Cleared {} completed futures", jobId, 0);
        } catch (TimeoutException e) {
            log.error("Job {}: Timed out waiting {}s for {} futures. Cancelling...", jobId, joinTimeoutSeconds, futures.size());
            futures.forEach(f -> f.cancel(true));
            meterRegistry.counter("node_import_join_timeout", "jobId", jobId.toString()).increment();
            futures.clear();
            throw new RuntimeException("Batch import join timed out after " + joinTimeoutSeconds + "s", e);
        } catch (Exception e) {
            log.error("Job {}: Error joining futures", jobId, e);
            throw new RuntimeException("Failed to join batch futures", e);
        }
    }


    private void finalizeJob(UUID jobId, String groupId, UUID domainId,
                             List<String> success, List<String> failed, int total, long startTime) {
        statusUpdater.updateTotalNodes(jobId, total);
        log.info("Job {}: Total parsed = {}, Success = {}, Failed = {}", jobId, total, success.size(), failed.size());

        if (total != success.size() + failed.size()) {
            log.error("Job {}: Discrepancy: parsed={}, success={}, failed={}", jobId, total, success.size(), failed.size());
            meterRegistry.counter("node_import_discrepancies", Constant.DOMAIN_ID, domainId.toString(), Constant.GROUP_ID, groupId).increment();
        }

        if (total == 0) {
            statusUpdater.failJob(jobId, groupId, "Parsed 0 nodes from uploaded file.", success, failed, total, domainId);
        } else if (!failed.isEmpty()) {
            statusUpdater.failJob(jobId, groupId, "Some nodes failed during processing.", success, failed, total, domainId);
        } else {
            statusUpdater.completeJob(jobId, groupId, success, total, domainId);
        }

        recordProcessingTime(domainId, groupId, startTime);
        meterRegistry.counter("node_import_processed", Constant.DOMAIN_ID, domainId.toString(), Constant.GROUP_ID, groupId).increment(total);
    }

    private void handleImportFailure(UUID jobId, UUID domainId, String groupId, IOException e) {
        log.error("Job {}: IO error during import", jobId, e);
        statusUpdater.handleUnexpectedFailure(jobId, domainId, groupId, e);
        meterRegistry.counter("node_import_io_errors", Constant.DOMAIN_ID, domainId.toString(), Constant.GROUP_ID, groupId).increment();
    }

    private void logImportStart(UUID jobId, String groupId, UUID domainId, MultipartFile file) {
        log.info("Job {} started | groupId={} | domainId={} | file={} | size={} bytes",
                jobId, groupId, domainId, file.getOriginalFilename(), file.getSize());
    }

    private void logExecutorState(UUID jobId) {
        log.info("Job {}: Executor state | Pool: {} | Queue: {} | Active: {}",
                jobId,
                executor.getPoolSize(),
                executor.getQueueSize(),
                executor.getActiveCount());
    }

    private void recordProcessingTime(UUID domainId, String groupId, long startTime) {
        long durationMs = (System.currentTimeMillis() - startTime / 1_000_000);
        log.info("Job finished | groupId={} | duration={}ms", groupId, durationMs);
        meterRegistry.timer("node_import_processing_time", Constant.DOMAIN_ID, domainId.toString(), Constant.GROUP_ID, groupId)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> processNodesImport(UUID jobId, List<String> referenceIds, String groupId, int batchSize, UUID domainId) {
        MatchingGroup group = groupConfigService.getGroupConfig(groupId, domainId);

        return CompletableFuture
                .supplyAsync(() -> {
                    log.info("Job {} started | groupId={} | domainId={} | refs={}", jobId, groupId, domainId, referenceIds.size());
                    statusUpdater.updateJobStatus(jobId, JobStatus.PROCESSING);

                    if (referenceIds.isEmpty()) {
                        statusUpdater.failJob(jobId, groupId, "No referenceIds provided.", List.of(), List.of(), 0, domainId);
                        return null;
                    }

                    statusUpdater.updateTotalNodes(jobId, referenceIds.size());
                    List<Node> nodes = GraphRequestFactory.createNodesFromReferences(referenceIds, group.getId(), NodeType.USER, domainId);
                    log.info("Job {}: Created {} nodes from refs", jobId, nodes.size());

                    List<List<Node>> batches = ListUtils.partition(nodes, batchSize);
                    List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());

                    for (List<Node> batch : batches) {
                        long timeoutMs = calculateBatchTimeout(batch.size());
                        futures.add(
                                CompletableFuture
                                        .runAsync(() -> {
                                            nodesImportProcessor.processAndPersist(jobId, groupId, batch, batchSize, referenceIds.size(), domainId);
                                            log.info("Job {}: Completed ref batch of {}", jobId, batch.size());
                                        }, executor)
                                        .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                                        .exceptionally(t -> {
                                            log.error("Job {}: Ref batch failed or timed out", jobId, t);
                                            return null;
                                        })
                        );
                    }

                    joinAndClearFutures(jobId, futures);
                    return null;
                }, executor)
                .handle((v, t) -> {
                    if (t != null) {
                        log.error("Job {}: Reference import failed", jobId, t);
                        statusUpdater.handleUnexpectedFailure(jobId, domainId, groupId, new RuntimeException(t));
                        meterRegistry.counter("node_import_errors", "domainId", domainId.toString(), "groupId", groupId).increment();
                    }
                    return null;
                });
    }
}