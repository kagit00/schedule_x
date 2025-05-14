package com.shedule.x.service;

import com.shedule.x.models.Node;

import java.util.List;
import java.util.UUID;

public interface GraphBuilderService {
    GraphBuilder.GraphResult buildSymmetric(List<Node> nodes, String weightFunctionKey, String groupId, UUID domainId);
    GraphBuilder.GraphResult build(List<Node> leftPartition, List<Node> rightPartition, String weightFunctionKey);
}
