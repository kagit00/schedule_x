package com.shedule.x.processors;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.dto.EdgeDTO;
import com.shedule.x.service.GraphRecords;
import com.shedule.x.utils.graph.StoreUtility;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
@RequiredArgsConstructor
public class EdgePersistenceFacade implements EdgePersistence {

    private final UnifiedWriteOrchestrator orchestrator;
    private final EdgeReader edgeReader;
    private final EdgeCleaner edgeCleaner;
    private final KeyPrefixProvider prefixProvider;

    @Override
    public CompletableFuture<Void> persistAsync(List<GraphRecords.PotentialMatch> matches,
                                                UUID groupId, int chunkIndex, String cycleId) {
        if (matches.isEmpty()) return CompletableFuture.completedFuture(null);
        return orchestrator.enqueueEdgeWrite(matches, groupId, cycleId);
    }

    @Override
    public CompletableFuture<Void> enqueueLshChunk(Long2ObjectMap<List<UUID>> chunk) {
        if (chunk == null || chunk.isEmpty()) return CompletableFuture.completedFuture(null);
        return orchestrator.enqueueLshWrite(chunk);
    }

    @Override
    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId, String cycleId) {
        ByteBuffer prefix = prefixProvider.makePrefix(groupId, cycleId);
        return edgeReader.streamEdges(domainId, prefix,
                k -> k.remaining() >= 32 && StoreUtility.keyStartsWith(k, prefix));
    }

    @Override
    public AutoCloseableStream<EdgeDTO> streamEdges(UUID domainId, UUID groupId) {
        ByteBuffer prefix = prefixProvider.makePrefix(groupId);
        return edgeReader.streamEdges(domainId, prefix,
                k -> StoreUtility.matchesGroupPrefix(k, groupId));
    }

    @Override
    public void cleanEdges(UUID groupId, String cycleId) {
        edgeCleaner.deleteByPrefix(prefixProvider.makePrefix(groupId, cycleId));
    }

    @Override
    public void close() throws Exception {
        orchestrator.shutdown();
    }
}