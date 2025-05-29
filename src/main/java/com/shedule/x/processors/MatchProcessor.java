package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface MatchProcessor {
    CompletableFuture<Void> processChunkMatches(GraphRecords.ChunkResult chunkResult, String groupId, UUID domainId, String processingCycleId, int matchBatchSize);
    CompletableFuture<Void> savePendingMatches(String groupId, UUID domainId, String processingCycleId, int batchSize);
    CompletableFuture<Void> saveFinalMatches(String groupId, UUID domainId, String processingCycleId, AutoCloseableStream<Edge> initialEdgeStream, int topK);
    long getFinalMatchCount(String groupId, UUID domainId, String processingCycleId);
    Set<String> getCachedMatchKeysForDomainAndGroup(String groupId, UUID domainId);
    AutoCloseableStream<Edge> streamEdges(String groupId, UUID domainId, String processingCycleId, int topK);
    void cleanup(String groupId);
}