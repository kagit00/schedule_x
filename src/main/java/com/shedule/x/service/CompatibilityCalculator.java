package com.shedule.x.service;

import com.shedule.x.models.Node;

public interface CompatibilityCalculator {
    double calculate(Node node1, Node node2);
}