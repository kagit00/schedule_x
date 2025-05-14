package com.shedule.x.processors;

import java.util.concurrent.CompletableFuture;

public interface PayloadProcessor {
    CompletableFuture<Void> process(String payload);
}