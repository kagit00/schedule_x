package com.shedule.x.service;

import com.shedule.x.dto.NodeExchange;

import java.util.concurrent.CompletableFuture;


public interface ImportJobService {
    CompletableFuture<Void> startNodesImport(NodeExchange payload);
}
