package com.shedule.x.metrics;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.utils.basic.Constant;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class GraphBuilderMetrics {
    private final MeterRegistry meterRegistry;
    private final DistributionSummary edgeWeightSummary;


    public GraphBuilderMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.edgeWeightSummary = DistributionSummary.builder("graph_edge_weight")
                .description("Compatibility score of generated edges")
                .register(meterRegistry);
    }

    public void recordEdgeWeight(double weight) {
        edgeWeightSummary.record(weight);
    }

    public void incrementMatchCount(long count, String groupId) {
        meterRegistry.counter("matches_generated", Constant.GROUP_ID, groupId).increment(count);
    }

    public void recordBuildError(String groupId, UUID domainId) {
        meterRegistry.counter("graph_build_error", "groupId", groupId, "domainId", domainId.toString()).increment();
    }

    public void recordBipartiteBuildError() {
        meterRegistry.counter("graph_build_errors", "type", MatchType.BIPARTITE.name()).increment();
    }

    public void recordBuildDuration(long durationMs, String groupId, String mode) {
        meterRegistry.timer("graph_build_duration", "groupId", groupId, "mode", mode)
                .record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public void recordBipartiteBuildDuration(long durationMs) {
        meterRegistry.timer("graph_build_duration", "type", MatchType.BIPARTITE.name())
                .record(durationMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    public Timer getTimer() {
        return meterRegistry.timer("your_timer_name");
    }
}