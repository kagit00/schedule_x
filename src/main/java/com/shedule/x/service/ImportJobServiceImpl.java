package com.shedule.x.service;

import com.shedule.x.dto.BytesMultipartFile;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.models.NodesImportJob;
import com.shedule.x.processors.NodesImportProcessor;
import com.shedule.x.repo.NodesImportJobRepository;
import com.shedule.x.utils.validation.FileValidationUtility;
import com.shedule.x.validation.NodeImportValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;
import static com.shedule.x.utils.basic.BasicUtility.parseFileContent;


@Service
@Slf4j
@RequiredArgsConstructor
public class ImportJobServiceImpl implements ImportJobService {

    private final NodesImportJobRepository nodesImportJobRepository;
    private final NodesImportProcessor nodesImportProcessor;
    private static final int BATCH_SIZE = 1000;


    @Override
    @Transactional
    public void startNodesImport(NodeExchange payload) {
        if (NodeImportValidator.isValidPayloadForCostBasedNodes(payload)) {
            UUID jobId = initiateNodesImport(payload);
            byte[] fileContent = parseFileContent(payload.getFileContent());
            MultipartFile file = new BytesMultipartFile(payload.getFileName(), payload.getContentType(), fileContent);

            FileValidationUtility.validateInput(file, payload.getGroupId());
            log.info("Starting node import for groupId={}, file size={}", payload.getGroupId(), file.getSize());
            nodesImportProcessor.processNodesImport(jobId, file, payload);

        } else if (NodeImportValidator.isValidPayloadForNonCostBasedNodes(payload)) {
            UUID jobId = initiateNodesImport(payload);
            var groupId = payload.getGroupId();
            var referenceIds = payload.getReferenceIds();
            var domainId = payload.getDomainId();

            log.info("Starting node import for groupId={}, number of nodes={}", groupId, referenceIds.size());
            nodesImportProcessor.processNodesImport(jobId, referenceIds, groupId, BATCH_SIZE, domainId);
        }
    }

    private UUID initiateNodesImport(NodeExchange message) {
        NodesImportJob job = GraphRequestFactory.createNodesImportJob(
                message.getGroupId(),
                message.getDomainId(),
                JobStatus.PENDING,
                0,
                0
        );
        nodesImportJobRepository.save(job);
        return job.getId();
    }
}