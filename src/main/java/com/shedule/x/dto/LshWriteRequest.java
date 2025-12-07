package com.shedule.x.dto;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public record LshWriteRequest(
            Long2ObjectMap<List<UUID>> chunk,
            CompletableFuture<Void> future,
            long enqueuedTime) implements WriteRequest {

        public LshWriteRequest(Long2ObjectMap<List<UUID>> chunk) {
            this(chunk, new CompletableFuture<>(), System.currentTimeMillis());
        }

        @Override public CompletableFuture<Void> future() { return future; }
        @Override public WriteType type() { return WriteType.LSH; }
        @Override public long enqueuedTime() { return enqueuedTime; }
    }