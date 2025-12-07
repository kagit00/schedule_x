package com.shedule.x.dto;

import com.shedule.x.service.GraphRecords;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public record EdgeWriteRequest(
            List<GraphRecords.PotentialMatch> matches,
            UUID groupId,
            String cycleId,
            CompletableFuture<Void> future,
            long enqueuedTime) implements WriteRequest {

        public EdgeWriteRequest(List<GraphRecords.PotentialMatch> matches, UUID groupId, String cycleId) {
            this(matches, groupId, cycleId, new CompletableFuture<>(), System.currentTimeMillis());
        }

        @Override public CompletableFuture<Void> future() { return future; }
        @Override public WriteType type() { return WriteType.EDGE; }
        @Override public long enqueuedTime() { return enqueuedTime; }
    }