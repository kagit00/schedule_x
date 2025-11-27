package com.shedule.x.config.factory;

import com.shedule.x.builder.FlatEdgeBuildingStrategy;
import com.shedule.x.builder.MetadataEdgeBuildingStrategy;
import com.shedule.x.builder.SymmetricEdgeBuildingStrategy;
import com.shedule.x.config.EdgeBuildingConfig;
import com.shedule.x.dto.NodeDTO;
import com.shedule.x.models.Node;
import com.shedule.x.processors.*;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.service.NodeDataService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Component
public class SymmetricEdgeBuildingStrategyFactory {
    private final FlatEdgeBuildingStrategy flatEdgeBuildingStrategy;
    private final ObjectProvider<LSHIndex> lshIndexProvider;
    private final MetadataEncoder encoder;
    private final Integer candidateLimit;
    private final Double similarityThreshold;
    private final ExecutorService executor;

    @Autowired
    private EdgeProcessor edgeProcessor;
    @Autowired
    private NodeDataService nodeDataService;

    @Autowired
    public SymmetricEdgeBuildingStrategyFactory(
            FlatEdgeBuildingStrategy flatEdgeBuildingStrategy,
            ObjectProvider<LSHIndex> lshIndexProvider,
            MetadataEncoder encoder,
            Integer candidateLimit,
            Double similarityThreshold,
            @Qualifier("graphBuildExecutor") ExecutorService executor) {
        this.flatEdgeBuildingStrategy = flatEdgeBuildingStrategy;
        this.lshIndexProvider = lshIndexProvider;
        this.candidateLimit = candidateLimit;
        this.similarityThreshold = similarityThreshold;
        this.encoder = encoder;
        this.executor = executor;
    }


    public SymmetricEdgeBuildingStrategy createStrategy(String weightFunctionKey, List<NodeDTO> nodes) {
        Map<UUID, NodeDTO> nodeMap = nodes.stream()
                .collect(Collectors.toMap(NodeDTO::getId, node -> node));
        if ("flat".equalsIgnoreCase(weightFunctionKey)) {
            return flatEdgeBuildingStrategy;
        } else {
            return new MetadataEdgeBuildingStrategy(
                    EdgeBuildingConfig.builder()
                            .candidateLimit(candidateLimit).similarityThreshold(similarityThreshold)
                            .chunkTimeoutSeconds(60).maxRetries(3).retryDelayMillis(10000)
                            .build(),
                    lshIndexProvider.getObject(),
                    encoder,
                    executor,
                    edgeProcessor,
                    nodeDataService
            );
        }
    }
}