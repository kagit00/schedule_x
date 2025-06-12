package com.shedule.x.cache;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public interface MatchCache {
    void cacheMatches(List<GraphRecords.PotentialMatch> matches, UUID groupId, UUID domainId, String batchId);
    AutoCloseableStream<Edge> streamEdges(UUID groupId, UUID domainId, String batchId, int topK);
    void clearMatches(UUID groupId);
    Set<String> getCachedMatchKeysForDomainAndGroup(UUID groupId, UUID domainId);
}