package com.shedule.x.processors;

import com.shedule.x.service.GraphRecords;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface EdgeWriter {
    CompletableFuture<Void> enqueueWrite(List<GraphRecords.PotentialMatch> matches, UUID groupId, String cycleId);
    void shutdown();
}