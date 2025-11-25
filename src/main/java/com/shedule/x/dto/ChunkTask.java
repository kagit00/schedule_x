package com.shedule.x.dto;

import com.shedule.x.models.Node;

import java.util.List;

public record ChunkTask(int sourceIndex, int targetIndex, List<NodeDTO> sourceNodes, List<NodeDTO> targetNodes) {}