package com.shedule.x.service;

import com.shedule.x.dto.NodeExchange;
import com.shedule.x.processors.NodesImportProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class NodesImportService {
    private final NodesImportProcessor nodesImportProcessor;

    @Async
    public void processNodesImport(UUID jobId, MultipartFile file, NodeExchange message) {
        nodesImportProcessor.process(jobId, file, message);
    }

    @Async
    public void processNodesImport(UUID jobId, List<String> referenceIds, String groupId, int batchSize, UUID domainId) {
        nodesImportProcessor.processNodesImport(jobId, referenceIds, groupId, batchSize, domainId);
    }
}
