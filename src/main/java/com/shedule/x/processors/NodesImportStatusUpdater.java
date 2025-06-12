package com.shedule.x.processors;

import com.shedule.x.async.ScheduleXProducer;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.dto.NodesTransferJobExchange;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.models.Domain;
import com.shedule.x.models.MatchingGroup;
import com.shedule.x.models.NodesImportJob;
import com.shedule.x.repo.NodesImportJobRepository;
import com.shedule.x.service.DomainService;
import com.shedule.x.service.GroupConfigService;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.Constant;
import com.shedule.x.utils.basic.StringConcatUtil;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class NodesImportStatusUpdater {
    private static final String USERS_TRANSFER_JOB_STATUS_TOPIC = "users-transfer-job-status-retrieval";

    private final GroupConfigService groupConfigService;
    private final NodesImportJobRepository nodesImportJobRepository;
    private final MeterRegistry meterRegistry;
    private final ScheduleXProducer scheduleXProducer;
    private final DomainService domainService;
    private final RetryTemplate retryTemplate;

    @Transactional
    public UUID initiateNodesImport(NodeExchange message) {
        MatchingGroup group = groupConfigService.getGroupConfig(message.getGroupId(), message.getDomainId());
        NodesImportJob job = GraphRequestFactory.createNodesImportJob(
                group.getId(),
                message.getDomainId(),
                JobStatus.PENDING,
                0,
                0
        );
        nodesImportJobRepository.save(job);
        return job.getId();
    }

    @Transactional
    public void updateJobStatus(UUID jobId, JobStatus status) {
        nodesImportJobRepository.updateStatus(jobId, status);
    }

    @Transactional
    public void updateTotalNodes(UUID jobId, int total) {
        nodesImportJobRepository.updateTotalNodes(jobId, total);
    }

    @Transactional
    public void completeJob(UUID jobId, String groupId, List<String> success, int total, UUID domainId) {
        log.info("Job {}: Marking as COMPLETED. Total nodes = {}, Successful = {}", jobId, total, success.size());
        nodesImportJobRepository.markCompleted(jobId, JobStatus.COMPLETED);
        NodesTransferJobExchange job = GraphRequestFactory.build(jobId, groupId, JobStatus.COMPLETED.name(), success.size(), total, success, List.of());
        sendJobStatusMessage(job, domainId);
        meterRegistry.counter("node_import_jobs_completed", Constant.GROUP_ID, groupId).increment();
    }

    @Transactional
    public void failJob(UUID jobId, String groupId, String reason, List<String> success, List<String> failed, int total, UUID domainId) {
        log.error("Job {} failed for groupId={} due to '{}'. Success = {}, Failed = {}", jobId, groupId, reason, success.size(), failed.size());
        nodesImportJobRepository.markFailed(jobId, JobStatus.FAILED, reason);
        NodesTransferJobExchange job = GraphRequestFactory.build(jobId, groupId, JobStatus.FAILED.name(), success.size(), total, success, failed);
        sendJobStatusMessage(job, domainId);
        meterRegistry.counter("node_import_jobs_failed", Constant.GROUP_ID, groupId).increment();
    }

    @Transactional
    public void handleUnexpectedFailure(UUID jobId, UUID domainId, String groupId, Throwable e) {
        log.error("Job {} failed unexpectedly for groupId={}: {}", jobId, groupId, e.getMessage(), e);
        nodesImportJobRepository.markFailed(jobId, JobStatus.FAILED, "Unexpected error: " + e.getMessage());
        int processed = nodesImportJobRepository.getProcessedNodes(jobId);
        int total = nodesImportJobRepository.getTotalNodes(jobId);

        NodesTransferJobExchange job = GraphRequestFactory.build(jobId, groupId, JobStatus.FAILED.name(), processed, total, List.of(), List.of());
        sendJobStatusMessage(job, domainId);
        meterRegistry.counter("node_import_unexpected_errors", "groupId", groupId).increment();
    }

    private void sendJobStatusMessage(NodesTransferJobExchange job, UUID domainId) {
        retryTemplate.execute(context -> {
            try {
                Domain domain = domainService.getDomainById(domainId);
                String domainName = domain.getName();
                job.setDomainId(domainId);
                String topic = StringConcatUtil.concatWithSeparator("-", domainName, USERS_TRANSFER_JOB_STATUS_TOPIC);
                String key = StringConcatUtil.concatWithSeparator("-", domainId.toString(), job.getGroupId());
                String value = BasicUtility.stringifyObject(job);

                scheduleXProducer.sendMessage(topic, key, value, true);
                return null;
            } catch (Exception e) {
                log.warn("Attempt {} failed to send job status for jobId={}: {}", context.getRetryCount() + 1, job.getJobId(), e.getMessage());
                throw e;
            }
        });
    }
}
