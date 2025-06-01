package com.shedule.x.builder;

import com.shedule.x.metrics.GraphBuilderMetrics;
import com.shedule.x.models.Edge;
import com.shedule.x.models.Node;
import com.shedule.x.processors.MetadataCompatibilityCalculator;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Set;

@Slf4j
@Component
public class BipartiteEdgeBuilder {
    private static final int MATCH_BATCH_SIZE = 250;

    private final MeterRegistry meterRegistry;
    private final GraphBuilderMetrics metrics;
    private final CompatibilityCalculator compatibilityCalculator;

    @Value("${bipartite.edge.build.similarity-threshold:0.05}")
    private Double similarityThreshold;

    @Getter
    @AllArgsConstructor
    public class EdgeBuildResult {
        private final Set<Edge> edges;
        private final List<GraphRecords.PotentialMatch> matches;
    }

    public BipartiteEdgeBuilder(
            MeterRegistry meterRegistry,
            GraphBuilderMetrics metrics) {
        this.meterRegistry = meterRegistry;
        this.metrics = metrics;
        this.compatibilityCalculator = new MetadataCompatibilityCalculator();
    }

    public void processBatch(
            List<Node> leftBatch,
            List<Node> rightBatch,
            List<GraphRecords.PotentialMatch> matches,
            Set<Edge> edges,
            String groupId,
            UUID domainId,
            Map<String, Object> context
    ) {
        Timer.Sample sample = Timer.start(meterRegistry);
        log.debug("Processing bipartite batch of {} left nodes vs {} right nodes for groupId={}",
                leftBatch.size(), rightBatch.size(), groupId);

        for (Node leftNode : leftBatch) {
            for (Node rightNode : rightBatch) {
                double score = compatibilityCalculator.calculate(leftNode, rightNode);
                if (score > similarityThreshold) {
                    Edge edge = Edge.builder().fromNode(leftNode).toNode(rightNode).weight(score).build();
                    edges.add(edge);
                    GraphRecords.PotentialMatch match = new GraphRecords.PotentialMatch(
                            leftNode.getReferenceId(),
                            rightNode.getReferenceId(),
                            score,
                            groupId,
                            domainId
                    );
                    matches.add(match);
                    metrics.recordEdgeWeight(score);
                }
            }
        }

        sample.stop(meterRegistry.timer("bipartite_builder_edge_compute", "groupId", groupId));
        meterRegistry.counter("matches_generated_total", "groupId", groupId, "mode", "bipartite").increment(matches.size());
        log.debug("Completed bipartite batch for groupId={}, generated {} matches", groupId, matches.size());
    }
}