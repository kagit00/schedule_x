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
        long startTime = System.nanoTime();
        List<Node> result = new ArrayList<>(responses.size());
        String messageGroupId = message.getGroupId();
        UUID domainId = message.getDomainId();

        for (NodeResponse res : responses) {
            String referenceId = res.getReferenceId();

            boolean isValidReference = referenceId != null && !referenceId.isEmpty();
            boolean isSameGroup = messageGroupId.equals(res.getGroupId());

            if (isValidReference && isSameGroup) {
                Node node = Node.builder()
                        .groupId(messageGroupId)
                        .domainId(domainId)
                        .referenceId(referenceId)
                        .type(res.getType() != null ? res.getType() : NodeType.USER)
                        .build();

                long metaStart = System.nanoTime();
                node.setMetaData(sanitizeMetadata(res));
                log.debug("Sanitized metadata for node {} in {} ms", referenceId, (System.nanoTime() - metaStart) / 1_000_000);

                node.setCreatedAt(DefaultValuesPopulator.getCurrentTimestamp());
                result.add(node);
            }
        }

        log.info("transformToEntities: {} valid nodes out of {} in {} ms",
                result.size(), responses.size(), (System.nanoTime() - startTime) / 1_000_000);
        return result;
    }


    private static Map<String, String> sanitizeMetadata(NodeResponse res) {
        long startTime = System.nanoTime();
        Map<String, String> metadata = res.getMetaData();
        if (metadata == null || metadata.isEmpty()) {
            return new HashMap<>();
        }
        Map<String, String> sanitized = new HashMap<>(metadata.size());
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key != null && value != null) {
                sanitized.put(ValueSanitizer.sanitize(key), ValueSanitizer.sanitize(value));
            }
        }
        log.debug("Sanitized {} metadata entries in {} ms", sanitized.size(), (System.nanoTime() - startTime) / 1_000_000);
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