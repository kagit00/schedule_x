package com.shedule.x.dto;

import java.util.concurrent.CompletableFuture;

public sealed interface WriteRequest permits EdgeWriteRequest, LshWriteRequest {
        CompletableFuture<Void> future();
        WriteType type();
        long enqueuedTime();
    }