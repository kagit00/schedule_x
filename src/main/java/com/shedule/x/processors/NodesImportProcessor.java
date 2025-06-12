package com.shedule.x.processors;

import com.shedule.x.dto.NodeExchange;
import com.shedule.x.dto.NodeResponse;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Node;
import com.shedule.x.repo.NodesImportJobRepository;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.service.GroupConfigService;
import com.shedule.x.utils.db.BatchUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class NodesImportProcessor {
    private final GroupConfigService groupConfigService;
    private final NodesImportJobRepository nodesImportJobRepository;
    private final NodeRepository nodeRepository;
    private final MeterRegistry meterRegistry;
    private final NodesStorageProcessor nodesStorageProcessor;
    private final NodesImportStatusUpdater statusUpdater;
    private final TransactionTemplate transactionTemplate;
    private final RetryTemplate retryTemplate;

    private final Object successLock = new Object();
    private final Object failedLock = new Object();

    public void processBatch(UUID jobId, List<NodeResponse> batch, NodeExchange message, List<String> success, List<String> failed, AtomicInteger totalParsed) {
        log.info("Job {}: Processing batch of size {}", jobId, batch.size());
        if (batch.isEmpty()) {
            log.warn("Job {}: Skipping empty batch", jobId);
            return;
        }

        retryTemplate.execute(context -> {
            long startTime = System.nanoTime();
            totalParsed.addAndGet(batch.size());
            List<Node> nodes = GraphRequestFactory.convertResponsesToNodes(batch, message, groupConfigService);
            log.info("Job {}: Converted {} nodes in {} ms", jobId, nodes.size(), (System.nanoTime() - startTime) / 1_000_000);

            if (!nodes.isEmpty()) {
                startTime = System.nanoTime();
                transactionTemplate.executeWithoutResult(status -> {
                    nodesStorageProcessor.saveNodesSafely(nodes);
                });
                long saveDuration = (System.nanoTime() - startTime) / 1_000_000;
                log.info("Job {}: Saved {} nodes in {} ms", jobId, nodes.size(), saveDuration);

                synchronized (successLock) {
                    nodes.forEach(n -> success.add(n.getReferenceId()));
                }

                startTime = System.nanoTime();
                transactionTemplate.executeWithoutResult(status -> {
                    nodesImportJobRepository.incrementProcessed(jobId, nodes.size());
                });
                log.info("Job {}: Incremented processed count in {} ms", jobId, (System.nanoTime() - startTime) / 1_000_000);

                meterRegistry.counter("node_import_batch_processed", "jobId", jobId.toString()).increment(batch.size());
            } else {
                log.warn("Job {}: Converted node list is empty for current batch", jobId);
                synchronized (failedLock) {
                    batch.forEach(r -> failed.add(r.getReferenceId()));
                }
            }
            return null;
        });
    }

    public void processAndPersist(UUID jobId, String groupId, List<Node> nodes, int batchSize, int total, UUID domainId) {
        retryTemplate.execute(context -> {
            transactionTemplate.executeWithoutResult(status -> {
                List<String> success = Collections.synchronizedList(new ArrayList<>());
                List<String> failed = Collections.synchronizedList(new ArrayList<>());

                BatchUtils.processInBatches(nodes, batchSize, batch -> {
                    log.info("Job {}: Persisting batch of size {}", jobId, batch.size());
                    long batchStartTime = System.nanoTime();
                    try {
                        nodeRepository.saveAll(batch);
                        long saveDuration = (System.nanoTime() - batchStartTime) / 1_000_000;
                        log.info("Job {}: Saved batch of {} nodes in {} ms", jobId, batch.size(), saveDuration);

                        synchronized (successLock) {
                            batch.forEach(n -> success.add(n.getReferenceId()));
                        }
                        nodesImportJobRepository.incrementProcessed(jobId, batch.size());
                        meterRegistry.counter("node_import_batch_processed", "jobId", jobId.toString()).increment(batch.size());
                    } catch (Exception e) {
                        synchronized (failedLock) {
                            batch.forEach(n -> failed.add(n.getReferenceId()));
                        }
                        log.warn("Job {}: Batch failed, skipping {} nodes: {}", jobId, batch.size(), e.getMessage());
                        throw new InternalServerErrorException("Batch persistence failed: " + e.getMessage());
                    }
                });

                if (!failed.isEmpty()) {
                    statusUpdater.failJob(jobId, groupId, "Some nodes failed during saving.", success, failed, total, domainId);
                } else {
                    statusUpdater.completeJob(jobId, groupId, success, total, domainId);
                }
            });
            return null;
        });
    }
}