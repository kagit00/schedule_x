package com.shedule.x.config.factory;

import com.shedule.x.dto.NodeResponse;
import com.shedule.x.dto.enums.NodeType;

import java.util.HashMap;
import java.util.Map;

public class NodeResponseFactory implements ResponseFactory<NodeResponse> {

    @Override
    public NodeResponse createResponse(NodeType type, String referenceId, Map<String, String> metadata, String groupId) {
        NodeResponse response = new NodeResponse();
        response.setType(type);
        response.setReferenceId(referenceId);
        response.setGroupId(groupId);
        response.setMetaData(metadata != null ? new HashMap<>(metadata) : new HashMap<>());
        return response;
    }
}