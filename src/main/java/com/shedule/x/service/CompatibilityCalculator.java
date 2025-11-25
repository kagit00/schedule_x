package com.shedule.x.service;

import com.shedule.x.dto.NodeDTO;
import com.shedule.x.models.Node;

public interface CompatibilityCalculator {
    double calculate(NodeDTO node1, NodeDTO node2);
}