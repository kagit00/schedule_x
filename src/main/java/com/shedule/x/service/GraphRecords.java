package com.shedule.x.service;

import com.shedule.x.models.Edge;
import com.shedule.x.dto.Graph;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface GraphRecords {

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    class PotentialMatch implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private String referenceId;
        private String matchedReferenceId;
        private double compatibilityScore;
        private UUID groupId;
        private UUID domainId;
    }

    @AllArgsConstructor
    @Data
    class ChunkResult {
        private final Set<Edge> edges;
        private final List<PotentialMatch> matches;
        private final int chunkIndex;
        private final Instant startTime;
    }

    @AllArgsConstructor
    @Data
    class GraphResult {
        private final Graph graph;
        private final List<PotentialMatch> potentialMatches;
    }
}
