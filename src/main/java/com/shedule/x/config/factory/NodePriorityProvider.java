package com.shedule.x.config.factory;

import java.util.UUID;

@FunctionalInterface
public interface NodePriorityProvider {
    long getPriority(UUID nodeId);
}