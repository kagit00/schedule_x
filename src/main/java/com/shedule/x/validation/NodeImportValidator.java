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

    public static boolean isValidPayloadForCostBasedNodes(NodeExchange message) {
        return message != null
                && message.getFilePath() != null
                && !message.getFilePath().isBlank()
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