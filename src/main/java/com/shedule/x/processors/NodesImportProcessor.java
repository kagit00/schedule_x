package com.shedule.x.processors;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.dto.NodesTransferJobExchange;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.dto.NodeResponse;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.config.factory.ResponseFactory;
import com.shedule.x.models.Domain;
import com.shedule.x.models.Node;
import com.shedule.x.repo.MatchingGroupRepository;
import com.shedule.x.repo.NodesImportJobRepository;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.service.DomainService;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.Constant;
import com.shedule.x.utils.basic.StringConcatUtil;
import com.shedule.x.utils.db.BatchUtils;
import com.shedule.x.utils.media.csv.CsvParser;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import static com.shedule.x.validation.NodeImportValidator.validateDomainAndGroup;


@Slf4j
@Component
@RequiredArgsConstructor
public class NodesImportProcessor {
    private static final String USERS_TRANSFER_JOB_STATUS_TOPIC = "users-transfer-job-status-retrieval";

    private final NodesImportJobRepository nodesImportJobRepository;
    private final NodeRepository nodeRepository;
    private final ResponseFactory<NodeResponse> nodeResponseFactory;
    private final MeterRegistry meterRegistry;
    private final ScheduleXProducer scheduleXProducer;
    private final MatchingGroupRepository matchingGroupRepository;
    private final DomainService domainService;

    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processNodesImport(UUID jobId, MultipartFile file, NodeExchange message) {
        String groupId = message.getGroupId();
        UUID domainId = message.getDomainId();

        try {
            log.info("Job {} started for groupId={}, domain_id={}, file name={}, size={} bytes",
                    jobId, groupId, domainId, file.getOriginalFilename(), file.getSize());

            nodesImportJobRepository.updateStatus(jobId, JobStatus.PROCESSING);
            validateDomainAndGroup(groupId, domainId, jobId, matchingGroupRepository);

            long startTime = System.currentTimeMillis();
            AtomicInteger totalParsed = new AtomicInteger(0);
            List<String> success = new ArrayList<>();
            List<String> failed = new ArrayList<>();

            CsvParser.parseInBatches(file.getInputStream(), nodeResponseFactory, batch -> {
                log.info("Job {}: Processing batch of size {}", jobId, batch.size());
                try {
                    if (batch.isEmpty()) {
                        log.warn("Job {}: Skipping empty batch", jobId);
                        return;
                    }

                    totalParsed.addAndGet(batch.size());
                    List<Node> nodes = GraphRequestFactory.convertResponsesToNodes(batch, message);

                    if (!nodes.isEmpty()) {
                        nodeRepository.saveAll(nodes);
                        nodes.forEach(n -> success.add(n.getReferenceId()));
                        nodesImportJobRepository.incrementProcessed(jobId, nodes.size());
                        log.info("Job {}: Successfully saved {} nodes", jobId, nodes.size());
                    } else {
                        log.warn("Job {}: Converted node list is empty for current batch", jobId);
                        batch.forEach(r -> failed.add(r.getReferenceId()));
                    }
                } catch (Exception e) {
                    log.error("Job {}: Error processing batch: {}", jobId, e.getMessage(), e);
                    batch.forEach(r -> failed.add(r.getReferenceId()));
                }
            });

            int total = totalParsed.get();
            nodesImportJobRepository.updateTotalNodes(jobId, total);
            log.info("Job {}: Total parsed nodes = {}, Success = {}, Failed = {}", jobId, total, success.size(), failed.size());

            if (total == 0) {
                failJob(jobId, groupId, "Parsed 0 nodes from uploaded file.", success, failed, total, domainId);
                return;
            }

            if (!failed.isEmpty()) {
                failJob(jobId, groupId, "Some nodes failed during processing.", success, failed, total, domainId);
            } else {
                completeJob(jobId, groupId, success, total, domainId);
            }

            recordProcessingTime(domainId, groupId, startTime);
        } catch (Exception e) {
            handleUnexpectedFailure(jobId, domainId, groupId, e);
            meterRegistry.counter("node_import_errors", Constant.DOMAIN_ID, domainId.toString(), Constant.GROUP_ID, groupId).increment();
        }
    }

    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processNodesImport(UUID jobId, List<String> referenceIds, String groupId, int batchSize, UUID domainId) {
        try {
            log.info("Job {} started for groupId={}, domain_id={}, referenceIds count={}",
                    jobId, groupId, domainId, referenceIds.size());
            nodesImportJobRepository.updateStatus(jobId, JobStatus.PROCESSING);
            validateDomainAndGroup(groupId, domainId, jobId, matchingGroupRepository);

            int total = referenceIds.size();
            if (total == 0) {
                failJob(jobId, groupId, "No referenceIds provided.", List.of(), List.of(), 0, domainId);
                return;
            }

            nodesImportJobRepository.updateTotalNodes(jobId, total);
            List<Node> nodes = GraphRequestFactory.createNodesFromReferences(referenceIds, groupId, NodeType.USER, domainId);
            log.info("Job {}: Created {} nodes from referenceIds", jobId, nodes.size());

            processAndPersist(jobId, groupId, nodes, batchSize, total, domainId);

        } catch (Exception e) {
            handleUnexpectedFailure(jobId, domainId, groupId, e);
            meterRegistry.counter("node_import_errors", "domain_id", domainId.toString(), "group_id", groupId).increment();
        }
    }

    private void processAndPersist(UUID jobId, String groupId, List<Node> nodes, int batchSize, int total, UUID domainId) {
        List<String> success = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        BatchUtils.processInBatches(nodes, batchSize, batch -> {
            log.info("Job {}: Persisting batch of size {}", jobId, batch.size());
            try {
                nodeRepository.saveAll(batch);
                batch.forEach(n -> success.add(n.getReferenceId()));
                nodesImportJobRepository.incrementProcessed(jobId, batch.size());
            } catch (Exception e) {
                batch.forEach(n -> failed.add(n.getReferenceId()));
                log.warn("Job {}: Batch failed, skipping {} nodes: {}", jobId, batch.size(), e.getMessage());
            }
        });

        if (!failed.isEmpty()) {
            failJob(jobId, groupId, "Some nodes failed during saving.", success, failed, total, domainId);
        } else {
            completeJob(jobId, groupId, success, total, domainId);
        }
    }

    private void completeJob(UUID jobId, String groupId, List<String> success, int total, UUID domainId) {
        log.info("Job {}: Marking as COMPLETED. Total nodes = {}, Successful = {}", jobId, total, success.size());
        nodesImportJobRepository.markCompleted(jobId, JobStatus.COMPLETED);
        NodesTransferJobExchange job = GraphRequestFactory.build(jobId, groupId, JobStatus.COMPLETED.name(), success.size(), total, success, List.of());
        sendJobStatusMessage(job, domainId);
    }

    private void failJob(UUID jobId, String groupId, String reason, List<String> success, List<String> failed, int total, UUID domainId) {
        log.error("Job {} failed for groupId={} due to '{}'. Success = {}, Failed = {}", jobId, groupId, reason, success.size(), failed.size());
        nodesImportJobRepository.markFailed(jobId, JobStatus.FAILED, reason);
        NodesTransferJobExchange job = GraphRequestFactory.build(jobId, groupId, JobStatus.FAILED.name(), 0, total, success, failed);
        sendJobStatusMessage(job, domainId);
    }

    private void sendJobStatusMessage(NodesTransferJobExchange job, UUID domainId) {
        Domain domain = domainService.getDomainById(domainId);
        String domainName = domain.getName();

        scheduleXProducer.sendMessage(
                StringConcatUtil.concatWithSeparator("-", domainName, USERS_TRANSFER_JOB_STATUS_TOPIC),
                StringConcatUtil.concatWithSeparator("-", domainId.toString(), job.getGroupId()),
                BasicUtility.stringifyObject(job)
        );
    }

    private void handleUnexpectedFailure(UUID jobId, UUID domainId, String groupId, Exception e) {
        log.error("Job {} failed unexpectedly for groupId={}: {}", jobId, groupId, e.getMessage(), e);

        nodesImportJobRepository.markFailed(jobId, JobStatus.FAILED, "Unexpected error: " + e.getMessage());
        int processed = nodesImportJobRepository.getProcessedNodes(jobId);
        int total = nodesImportJobRepository.getTotalNodes(jobId);

        NodesTransferJobExchange job = GraphRequestFactory.build(jobId, groupId, JobStatus.FAILED.name(), processed, total, List.of(), List.of());
        sendJobStatusMessage(job, domainId);
    }

    private void recordProcessingTime(UUID domainId, String groupId, long startTime) {
        long duration = System.currentTimeMillis() - startTime;
        log.info("Finished processing for groupId={}, duration={}ms", groupId, duration);

        meterRegistry.timer("node_import_processing_time", "domain_id", domainId.toString(), "group_id", groupId)
                .record(duration, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
}
