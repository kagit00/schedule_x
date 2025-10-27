package com.shedule.x.service;

import com.shedule.x.dto.NodeExchange;
import com.shedule.x.config.factory.GraphRequestFactory;
import com.shedule.x.processors.NodesImportStatusUpdater;
import com.shedule.x.utils.validation.FileValidationUtility;
import com.shedule.x.validation.NodeImportValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


@Slf4j
@Service
@RequiredArgsConstructor
public class ImportJobServiceImpl implements ImportJobService {
    private final NodesImportService nodesImportService;
    private final NodesImportStatusUpdater statusUpdater;

    @Value("${import.batch-size:1000}")
    private int batchSize;

    @Override
    public CompletableFuture<Void> startNodesImport(NodeExchange payload) {
        UUID jobId = statusUpdater.initiateNodesImport(payload);

        if (NodeImportValidator.isValidPayloadForCostBasedNodes(payload)) {
            MultipartFile file = GraphRequestFactory.resolvePayload(payload);
            FileValidationUtility.validateInput(file, payload.getGroupId());
            log.info("Starting node import for groupId={}, file size={}", payload.getGroupId(), file.getSize());
            return nodesImportService.processNodesImport(jobId, file, payload);

        } else if (NodeImportValidator.isValidPayloadForNonCostBasedNodes(payload)) {
            log.info("Starting node import for groupId={}, number of nodes={}", payload.getGroupId(), payload.getReferenceIds().size());
            return nodesImportService.processNodesImport(jobId, payload.getReferenceIds(), payload.getGroupId(), batchSize, payload.getDomainId());

        } else {
            log.warn("Invalid payload for groupId={}: {}", payload.getGroupId(), payload);
            return CompletableFuture.completedFuture(null);
        }
    }
}