package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface EdgePersistence extends AutoCloseable {
    CompletableFuture<Void> persistAsync(List<GraphRecords.PotentialMatch> matches, UUID groupId, int chunkIndex, String cycleId);
    AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId, String cycleId);
    AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId);
    void cleanEdges(UUID groupId, String cycleId);
}
