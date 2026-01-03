package com.shedule.x.config.factory;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.config.QueueManagerConfig;
import com.shedule.x.dto.*;
import com.shedule.x.dto.enums.JobStatus;
import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.models.*;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.service.GroupConfigService;
import com.shedule.x.utils.basic.DefaultValuesPopulator;
import com.shedule.x.utils.media.csv.ValueSanitizer;
import io.minio.MinioClient;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;


@Slf4j
@UtilityClass
public final class GraphRequestFactory {

    public static MatchingContext buildMatchingContext(UUID groupId, UUID domainId, int nodeCount, MatchType matchType, boolean isCostBased, String industry, MatchingRequest request) {
        return MatchingContext.builder()
                .domainId(domainId)
                .sizeOfNodes(nodeCount)
                .isRealTime(request.isRealTime())
                .isCostBased(isCostBased)
                .isSchedulingBased(true)
                .industry(industry)
                .matchType(matchType)
                .build();
    }

    public static NodesImportJob createNodesImportJob(UUID groupId, UUID domainId, JobStatus status, int processed, int total) {
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

    public static PotentialMatchEntity convertToPotentialMatch(EdgeDTO edge, UUID groupId, UUID domainId, String pid) {
        return PotentialMatchEntity.builder()
                .matchedAt(DefaultValuesPopulator.getCurrentTimestamp())
                .matchedReferenceId(edge.getToNodeHash())
                .referenceId(edge.getFromNodeHash())
                .compatibilityScore((double) edge.getScore())
                .groupId(groupId)
                .domainId(domainId)
                .processingCycleId(pid)
                .build();
    }

    public static String bucketNodes(int numberOfNodes) {
        if (numberOfNodes <= 100) return "0-100";
        if (numberOfNodes <= 500) return "101-500";
        return "501+";
    }


    public static List<Node> convertResponsesToNodes(List<NodeResponse> responses, NodeExchange message, GroupConfigService groupConfigService) {
        long startTime = System.nanoTime();
        List<Node> result = new ArrayList<>(responses.size());
        String messageGroupId = message.getGroupId();
        UUID domainId = message.getDomainId();

        for (NodeResponse res : responses) {
            String referenceId = res.getReferenceId();

            boolean isValidReference = referenceId != null && !referenceId.isEmpty();
            boolean isSameGroup = messageGroupId.equals(res.getGroupId());
            MatchingGroup group = groupConfigService.getGroupConfig(messageGroupId, domainId);

            if (isValidReference && isSameGroup) {
                Node node = Node.builder()
                        .groupId(group.getId())
                        .domainId(domainId)
                        .referenceId(referenceId)
                        .type(res.getType() != null ? res.getType().name() : NodeType.USER.name())
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

    public static List<Node> createNodesFromReferences(List<String> referenceIds, UUID groupId, NodeType type, UUID domainId) {
        List<Node> nodes = new ArrayList<>();
        for (String refId : referenceIds) {
            if (refId != null && !refId.isEmpty()) {
                Node node = Node.builder()
                        .groupId(groupId)
                        .domainId(domainId)
                        .referenceId(refId)
                        .type(type.name())
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

    public static RemoteMultipartFile fromRemotePayload(NodeExchange payload, MinioClient minioClient) {
        return new RemoteMultipartFile(payload.getFilePath(), payload.getFileName(), payload.getContentType(), minioClient);
    }

    public static PotentialMatchEntity convertToPotentialMatch(GraphRecords.PotentialMatch match) {
        return PotentialMatchEntity.builder()
                .groupId(match.getGroupId())
                .domainId(match.getDomainId())
                .referenceId(match.getReferenceId())
                .matchedReferenceId(match.getMatchedReferenceId())
                .compatibilityScore(match.getCompatibilityScore())
                .build();
    }

    public static Edge toEdge(GraphRecords.PotentialMatch match) {
        Node fromNode = Node.builder()
                .id(UUID.randomUUID())
                .referenceId(match.getReferenceId())
                .groupId(match.getGroupId())
                .domainId(match.getDomainId())
                .type("match")
                .createdAt(DefaultValuesPopulator.getCurrentTimestamp())
                .build();

        Node toNode = Node.builder()
                .id(UUID.randomUUID())
                .referenceId(match.getMatchedReferenceId())
                .groupId(match.getGroupId())
                .domainId(match.getDomainId())
                .type("match")
                .createdAt(DefaultValuesPopulator.getCurrentTimestamp())
                .build();

        return Edge.builder()
                .fromNode(fromNode)
                .toNode(toNode)
                .weight(match.getCompatibilityScore())
                .metaData(new HashMap<>())
                .build();
    }

    public static QueueConfig getQueueConfig(UUID groupId, UUID domainId, String processingCycleId, QueueManagerConfig queueManagerConfig) {
        return QueueConfig.builder()
                .groupId(groupId).processingCycleId(processingCycleId)
                .capacity(queueManagerConfig.capacity()).boostBatchFactor(queueManagerConfig.boostBatchFactor())
                .flushIntervalSeconds(queueManagerConfig.flushIntervalSeconds()).maxFinalBatchSize(queueManagerConfig.maxFinalBatchSize())
                .drainWarningThreshold(queueManagerConfig.drainWarningThreshold()).domainId(domainId)
                .build();
    }

    public static MultipartFile resolvePayload(NodeExchange payload, MinioClient minioClient) {
        String path = payload.getFilePath();
        if (path == null) throw new IllegalArgumentException("File path cannot be null in payload");
        if (path.startsWith("http://") || path.startsWith("https://")) return fromRemotePayload(payload, minioClient);
        return fromPayload(payload);
    }

}