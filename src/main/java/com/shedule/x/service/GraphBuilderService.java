package com.shedule.x.service;

import com.shedule.x.models.Node;

import java.util.List;

public interface GraphBuilderService {
    GraphBuilder.GraphResult buildSymmetric(List<Node> nodes, String weightFunctionKey, String groupId);
    GraphBuilder.GraphResult build(List<Node> leftPartition, List<Node> rightPartition, String weightFunctionKey);
}
