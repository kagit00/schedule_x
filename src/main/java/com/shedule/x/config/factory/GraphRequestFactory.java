package com.shedule.x.config.factory;

import com.shedule.x.dto.NodesTransferJobExchange;
import com.shedule.x.dto.NodeExchange;
import com.shedule.x.dto.NodeResponse;
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
    private static final long MAX_INPUT_SIZE = 500_000_000; // 500 MB
    private static final long MAX_JSON_SIZE = 50_000_000; // 50 MB

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

    public static NodeExchange build(String groupId, byte[] fileContent, String fileName, String contentType, UUID domainId) {
        if (fileContent.length > MAX_INPUT_SIZE) {
            throw new BadRequestException("Input fileContent too large: " + fileContent.length + " bytes");
        }

        byte[] compressedContent = compress(fileContent);
        String base64Content = Base64.getEncoder().encodeToString(compressedContent);

        NodeExchange message = NodeExchange.builder()
                .groupId(groupId)
                .fileContent(base64Content)
                .fileName(fileName)
                .contentType(contentType)
                .domainId(domainId)
                .build();

        String json = BasicUtility.stringifyObject(message);
        if (json.length() > MAX_JSON_SIZE) {
            throw new InternalServerErrorException("Serialized NodeExportMessage too large: " + json.length() + " bytes");
        }

        return message;
    }

    private static byte[] compress(byte[] data) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(byteArrayOutputStream)) {
            gzip.write(data);
            gzip.finish();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new InternalServerErrorException("Failed to compress data: " + e.getMessage());
        }
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
}