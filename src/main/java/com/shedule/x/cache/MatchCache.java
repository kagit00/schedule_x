package com.shedule.x.cache;

import com.shedule.x.config.factory.AutoCloseableStream;
import com.shedule.x.models.Edge;
import com.shedule.x.service.GraphRecords;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public interface MatchCache {
    void cacheMatches(List<GraphRecords.PotentialMatch> matches, String groupId, UUID domainId, String batchId);
    AutoCloseableStream<Edge> streamEdges(String groupId, UUID domainId, String batchId, int topK);
    void clearMatches(String groupId);
    Set<String> getCachedMatchKeysForDomainAndGroup(String groupId, UUID domainId);
}