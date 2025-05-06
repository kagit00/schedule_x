package com.shedule.x.config.factory;

import com.shedule.x.dto.*;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.exceptions.InternalServerErrorException;
import com.shedule.x.models.Node;
import com.shedule.x.models.NodesImportJob;
import com.shedule.x.utils.basic.BasicUtility;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import com.shedule.x.utils.media.csv.ValueSanitizer;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPOutputStream;

@Slf4j
public final class GraphRequestFactory {
    private static final long MAX_INPUT_SIZE = 500_000_000;
    private static final long MAX_JSON_SIZE = 50_000_000;

    private GraphRequestFactory() {
        throw new UnsupportedOperationException("unsupported");
    }

    public static NodesImportJob createNodesImportJob(String groupId, UUID domainId, JobStatus status, int processed, int total) {
        return NodesImportJob.builder()
                .groupId(groupId)
                .processedNodes(processed).domainId(domainId)
                .totalNodes(total).status(status)
                .startedAt(DefaultValuesPopulator.getCurrentTimestamp())
                .build();
    }

    public static MatchSuggestionsExchange buildFileReference(String groupId, String filePath, String fileName, String contentType, UUID domainId) {
        return MatchSuggestionsExchange.builder()
                .groupId(groupId)
                .domainId(domainId)
                .filePath(filePath)
                .fileName(fileName)
                .contentType(contentType)
                .build();
    }


    public static List<Node> convertResponsesToNodes(List<NodeResponse> responses, NodeExchange message) {
        List<Node> result = new ArrayList<>();
        for (int i = 0; i < responses.size(); i++) {
            NodeResponse res = responses.get(i);
            try {
                if (res.getReferenceId() == null || res.getReferenceId().isEmpty()) {
                    log.warn("Skipping node at index {} due to missing referenceId", i);
                    continue;
                }
                if (!message.getGroupId().equals(res.getGroupId())) {
                    log.warn("Skipping node at index {} due to groupId mismatch: expected {}, got {}", 
                            i, message.getGroupId(), res.getGroupId());
                    continue;
                }

                Node node = Node.builder()
                        .groupId(message.getGroupId()).domainId(message.getDomainId())
                        .referenceId(res.getReferenceId())
                        .type(res.getType() != null ? res.getType() : NodeType.USER)
                        .build();

                Map<String, String> sanitizedMetaData = sanitizeMetadata(res);
                node.setMetaData(sanitizedMetaData != null ? sanitizedMetaData : new HashMap<>());
                node.setCreatedAt(DefaultValuesPopulator.getCurrentTimestamp());
                result.add(node);
            } catch (Exception e) {
                log.error("Error transforming node at index {}: {}", i, e.getMessage(), e);
            }
        }
        log.info("transformToEntities: {} valid nodes out of {}", result.size(), responses.size());
        return result;
    }

    private static Map<String, String> sanitizeMetadata(NodeResponse res) {
        Map<String, String> metadata = res.getMetaData();
        if (metadata == null) {
            return new HashMap<>();
        }
        Map<String, String> sanitized = new HashMap<>();
        metadata.forEach((key, value) -> {
            if (key != null && value != null) {
                sanitized.put(ValueSanitizer.sanitize(key), ValueSanitizer.sanitize(value));
            }
        });
        return sanitized;
    }

    public static List<Node> createNodesFromReferences(List<String> referenceIds, String groupId, NodeType type, UUID domainId) {
        List<Node> nodes = new ArrayList<>();
        for (String refId : referenceIds) {
            if (refId != null && !refId.isEmpty()) {
                Node node = Node.builder()
                        .groupId(groupId)
                        .domainId(domainId)
                        .referenceId(refId)
                        .type(type)
                        .metaData(new HashMap<>())
                        .createdAt(DefaultValuesPopulator.getCurrentTimestamp())
                        .build();
                nodes.add(node);
            }
        }
        return nodes;
    }

    public static NodesTransferJobExchange build(UUID jobId, String groupId, String status, int processedNodes, int totalNodes,
                                                 List<String> successList, List<String> failedList) {
        return NodesTransferJobExchange.builder()
                .status(status).jobId(jobId)
                .total(totalNodes).processed(processedNodes)
                .failedList(failedList).successList(successList)
                .groupId(groupId)
                .build();
    }

    public static FileSystemMultipartFile fromPayload(NodeExchange payload) {
        return new FileSystemMultipartFile(payload.getFilePath(), payload.getFileName(), payload.getContentType());
    }
}