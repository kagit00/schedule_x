package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface PotentialMatchComputationProcessor {
    CompletableFuture<Void> processChunkMatches(GraphRecords.ChunkResult chunkResult, UUID groupId, UUID domainId, String processingCycleId, int matchBatchSize);
    CompletableFuture<Void> savePendingMatchesAsync(UUID groupId, UUID domainId, String processingCycleId, int batchSize);
    CompletableFuture<Void> saveFinalMatches(UUID groupId, UUID domainId, String processingCycleId, AutoCloseableStream<Edge> initialEdgeStream, int topK);
    long getFinalMatchCount(UUID groupId, UUID domainId, String processingCycleId);
    AutoCloseableStream<EdgeDTO> streamEdges(UUID groupId, UUID domainId, String processingCycleId, int topK);
    void cleanup(UUID groupId);
}