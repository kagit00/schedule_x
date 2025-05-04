package com.shedule.x.config.factory;

import com.shedule.x.dto.enums.NodeType;

import java.util.Map;

public interface ResponseFactory<T> {
    T createResponse(NodeType type, String referenceId, Map<String, String> metadata, String groupId);
}