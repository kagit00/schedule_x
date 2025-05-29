package com.shedule.x.service;

import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.config.factory.ResponseFactory;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.dto.NodeResponse;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.exceptions.InternalServerErrorException;
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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class NodesImportService {
    @Value("${import.node-timeout-ms:500}")
    private long nodeTimeoutMs;

    @Value("${import.max-parallel-futures:4}")
    private int maxParallelFutures;

    private final NodesImportStatusUpdater statusUpdater;
    private final NodesImportProcessor nodesImportProcessor;
    private final ResponseFactory<NodeResponse> nodeResponseFactory;
    private final MeterRegistry meterRegistry;
    private final ThreadPoolTaskExecutor executor;

    public NodesImportService(NodesImportStatusUpdater statusUpdater,
                              NodesImportProcessor nodesImportProcessor,
                              ResponseFactory<NodeResponse> nodeResponseFactory,
                              MeterRegistry meterRegistry,
                              @Qualifier("nodesImportExecutor") ThreadPoolTaskExecutor executor) {
        this.statusUpdater = statusUpdater;
        this.nodesImportProcessor = nodesImportProcessor;
        this.nodeResponseFactory = nodeResponseFactory;
        this.meterRegistry = meterRegistry;
        this.executor = executor;
    }

    public CompletableFuture<Void> processNodesImport(UUID jobId, MultipartFile file, NodeExchange message) {
        String groupId = message.getGroupId();
        UUID domainId = message.getDomainId();

        return CompletableFuture.supplyAsync(() -> {
            logImportStart(jobId, groupId, domainId, file);
            statusUpdater.updateJobStatus(jobId, JobStatus.PROCESSING);

            long startTime = System.nanoTime();
            AtomicInteger totalParsed = new AtomicInteger(0);
            List<String> success = Collections.synchronizedList(new ArrayList<>());
            List<String> failed = Collections.synchronizedList(new ArrayList<>());

            try (InputStream gzipStream = new GZIPInputStream(file.getInputStream())) {
                processBatchesFromStream(jobId, message, gzipStream, totalParsed, success, failed, startTime);
            } catch (IOException e) {
                handleImportFailure(jobId, domainId, groupId, e);
                return null;
            }

            finalizeJob(jobId, groupId, domainId, success, failed, totalParsed.get(), startTime);
            return null;
        }, executor).handle((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to process nodes import for jobId={}: {}", jobId, throwable.getMessage(), throwable);
                statusUpdater.handleUnexpectedFailure(jobId, domainId, groupId, throwable);
            }
            return null;
        });
    }

    private void processBatchesFromStream(UUID jobId, NodeExchange message,
                                          InputStream gzipStream, AtomicInteger totalParsed,
                                          List<String> success, List<String> failed, long startTime) {
        List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());

        log.info("Job {}: Executor queue size: {}, active threads: {}", jobId, executor.getQueueSize(), executor.getActiveCount());
        CsvParser.parseInBatches(gzipStream, nodeResponseFactory, batch -> {
            futures.add(processBatchAsync(jobId, batch, message, success, failed, totalParsed, startTime));
            if (futures.size() >= maxParallelFutures) {
                joinAndClearFutures(futures);
            }
        });

        joinAndClearFutures(futures);
    }

    private CompletableFuture<Void> processBatchAsync(UUID jobId, List<NodeResponse> batch, NodeExchange message,
                                                      List<String> success, List<String> failed,
                                                      AtomicInteger totalParsed, long startTime) {
        return CompletableFuture.runAsync(() -> {
            try {
                nodesImportProcessor.processBatch(jobId, batch, message, success, failed, totalParsed);
                long durationMs = (System.nanoTime() - startTime) / 1_000_000;
                log.info("Job {}: Completed batch of {} nodes in {} ms", jobId, batch.size(), durationMs);
            } catch (Exception e) {
                log.error("Job {}: Batch failed: {}", jobId, e.getMessage(), e);
                batch.forEach(r -> failed.add(r.getReferenceId()));
                throw new InternalServerErrorException("Batch processing failed for jobId=" + jobId + ": " + e.getMessage());
            }
        }, executor).whenComplete((result, throwable) -> {
            if (throwable != null) {
                if (throwable instanceof TimeoutException) {
                    log.error("Job {}: Batch processing timed out after {} ms", jobId, nodeTimeoutMs);
                    batch.forEach(r -> failed.add(r.getReferenceId()));
                } else {
                    log.error("Job {}: Batch processing failed: {}", jobId, throwable.getMessage(), throwable);
                }
            }
        }).orTimeout(nodeTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void joinAndClearFutures(List<CompletableFuture<Void>> futures) {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenAcceptAsync(v -> {
                    futures.clear();
                    resultFuture.complete(null);
                }, executor)
                .exceptionally(t -> {
                    log.error("Failed to complete futures: {}", t.getMessage());
                    resultFuture.completeExceptionally(t);
                    return null;
                });

        try {
            resultFuture.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Error waiting for futures: {}", e.getMessage());
            throw new RuntimeException("Failed to join and clear futures", e);
        }
    }

    private void finalizeJob(UUID jobId, String groupId, UUID domainId,
                             List<String> success, List<String> failed, int total, long startTime) {
        statusUpdater.updateTotalNodes(jobId, total);
        log.info("Job {}: Total parsed nodes = {}, Success = {}, Failed = {}", jobId, total, success.size(), failed.size());

        if (total != success.size() + failed.size()) {
            log.error("Job {}: Discrepancy detected: parsed={}, success={}, failed={}", jobId, total, success.size(), failed.size());
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
        statusUpdater.handleUnexpectedFailure(jobId, domainId, groupId, e);
        meterRegistry.counter("node_import_errors", Constant.DOMAIN_ID, domainId.toString(), Constant.GROUP_ID, groupId).increment();
    }

    private void logImportStart(UUID jobId, String groupId, UUID domainId, MultipartFile file) {
        log.info("Job {} started for groupId={}, domainId={}, fileName={}, size={} bytes",
                jobId, groupId, domainId, file.getOriginalFilename(), file.getSize());
    }

    public CompletableFuture<Void> processNodesImport(UUID jobId, List<String> referenceIds, String groupId, int batchSize, UUID domainId) {
        return CompletableFuture.supplyAsync(() -> {
            log.info("Job {} started for groupId={}, domainId={}, referenceIds count={}", jobId, groupId, domainId, referenceIds.size());
            statusUpdater.updateJobStatus(jobId, JobStatus.PROCESSING);

            if (referenceIds.isEmpty()) {
                statusUpdater.failJob(jobId, groupId, "No referenceIds provided.", List.of(), List.of(), 0, domainId);
                return null;
            }

            statusUpdater.updateTotalNodes(jobId, referenceIds.size());

            List<Node> nodes = GraphRequestFactory.createNodesFromReferences(referenceIds, groupId, NodeType.USER, domainId);
            log.info("Job {}: Created {} nodes from referenceIds", jobId, nodes.size());

            List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());
            List<List<Node>> batches = ListUtils.partition(nodes, batchSize);

            for (List<Node> batch : batches) {
                log.info("Job {}: Submitting batch of size {}", jobId, batch.size());
                futures.add(CompletableFuture.runAsync(() -> {
                    nodesImportProcessor.processAndPersist(jobId, groupId, batch, batchSize, referenceIds.size(), domainId);
                    log.info("Job {}: Completed batch of size {}", jobId, batch.size());
                }, executor).orTimeout(nodeTimeoutMs, TimeUnit.MILLISECONDS));
            }

            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenAcceptAsync(v -> resultFuture.complete(null), executor)
                    .exceptionally(t -> {
                        log.error("Failed to complete futures for jobId={}: {}", jobId, t.getMessage());
                        resultFuture.completeExceptionally(t);
                        return null;
                    });

            try {
                resultFuture.get(nodeTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.error("Error waiting for futures for jobId={}: {}", jobId, e.getMessage());
                throw new RuntimeException(e);
            }
            return null;
        }, executor).handle((result, throwable) -> {
            if (throwable != null) {
                log.error("Failed to process nodes import for jobId={}: {}", jobId, throwable.getMessage(), throwable);
                statusUpdater.handleUnexpectedFailure(jobId, domainId, groupId, new RuntimeException(throwable));
                meterRegistry.counter("node_import_errors", "domainId", domainId.toString(), "groupId", groupId).increment();
            }
            return null;
        });
    }

    private void recordProcessingTime(UUID domainId, String groupId, long startTime) {
        long duration = (System.currentTimeMillis() - startTime) / 1_000_000;
        log.info("Finished processing for groupId={}, duration={}ms", groupId, duration);
        meterRegistry.timer("node_import_processing_time", "domainId", domainId.toString(), "groupId", groupId)
                .record(duration, TimeUnit.MILLISECONDS);
    }
}