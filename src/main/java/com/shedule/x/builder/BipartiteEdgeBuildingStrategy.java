package com.shedule.x.builder;

import com.shedule.x.dto.NodeDTO;
import com.shedule.x.metrics.GraphBuilderMetrics;
import com.shedule.x.models.Edge;
import com.shedule.x.processors.MetadataCompatibilityCalculator;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.service.GraphRecords;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

@Slf4j
@Component
public class BipartiteEdgeBuildingStrategy {

    private final MeterRegistry meterRegistry;
    private final GraphBuilderMetrics metrics;
    private final CompatibilityCalculator compatibilityCalculator;

    @Value("${bipartite.edge.build.similarity-threshold:0.05}")
    private Double similarityThreshold;

    public BipartiteEdgeBuildingStrategy(
            MeterRegistry meterRegistry,
            GraphBuilderMetrics metrics) {
        this.meterRegistry = meterRegistry;
        this.metrics = metrics;
        this.compatibilityCalculator = new MetadataCompatibilityCalculator();
    }

    public void processBatch(
            List<NodeDTO> leftBatch,
            List<NodeDTO> rightBatch,
            List<GraphRecords.PotentialMatch> matches,
            Set<Edge> edges,
            UUID groupId,
            UUID domainId,
            Map<String, Object> context
    ) {
        @SuppressWarnings("unchecked")
        Supplier<Boolean> isCancelled = (context != null && context.containsKey("cancellationCheck"))
                ? (Supplier<Boolean>) context.get("cancellationCheck")
                : () -> false;

        if (isCancelled.get() || Thread.currentThread().isInterrupted()) {
            throw new RuntimeException("Build cancelled by user/system");
        }

        Timer.Sample sample = Timer.start(meterRegistry);

        for (NodeDTO leftNode : leftBatch) {
            if (Thread.currentThread().isInterrupted() || isCancelled.get()) {
                log.warn("Bipartite batch processing cancelled/interrupted | groupId={}", groupId);
                throw new RuntimeException("Operation cancelled");
            }

            for (NodeDTO rightNode : rightBatch) {
                double score = compatibilityCalculator.calculate(leftNode, rightNode);
                if (score > similarityThreshold) {
                    matches.add(new GraphRecords.PotentialMatch(
                            leftNode.getReferenceId(),
                            rightNode.getReferenceId(),
                            score,
                            groupId,
                            domainId
                    ));
                    metrics.recordEdgeWeight(score);
                }
            }
        }

        sample.stop(meterRegistry.timer("bipartite_builder_edge_compute", "groupId", groupId.toString()));
        meterRegistry.counter("matches_generated_total", "groupId", groupId.toString(), "mode", "bipartite").increment(matches.size());
        log.debug("Completed bipartite batch for groupId={}, generated {} matches", groupId, matches.size());
    }
}