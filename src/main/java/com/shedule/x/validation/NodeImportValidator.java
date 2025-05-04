package com.shedule.x.validation;

import com.shedule.x.dto.NodeExchange;
import com.shedule.x.exceptions.BadRequestException;
import com.shedule.x.repo.MatchingGroupRepository;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

@Slf4j
public final class NodeImportValidator {

    private NodeImportValidator() {
        throw new UnsupportedOperationException("unsupported");
    }

    public static void validateDomainAndGroup(String groupId, UUID domainId, UUID jobId, MatchingGroupRepository matchingGroupRepository) {
        if (groupId == null || groupId.isBlank()) {
            String error = "Invalid groupId: null or blank";
            log.error("Job {}: {}", jobId, error);
            throw new BadRequestException(error);
        }
        if (matchingGroupRepository.findByDomainIdAndGroupId(domainId, groupId) == null) {
            String error = String.format("Invalid domainId=%s or groupId=%s", domainId, groupId);
            log.error("Job {}: {}", jobId, error);
            throw new BadRequestException(error);
        }
        log.info("Job {}: Validated domainId={} and groupId={}", jobId, domainId, groupId);
    }

    public static boolean isValidPayloadForCostBasedNodes(NodeExchange message) {
        return message != null
                && message.getFileContent() != null
                && !message.getFileContent().isBlank()
                && message.getFileName() != null
                && !message.getFileName().isBlank();
    }

    public static boolean isValidPayloadForNonCostBasedNodes(NodeExchange message) {
        return message != null
                && message.getGroupId() != null
                && !message.getReferenceIds().isEmpty()
                && message.getDomainId() != null;
    }
}