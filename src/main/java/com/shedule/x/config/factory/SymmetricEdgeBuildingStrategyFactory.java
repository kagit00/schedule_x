package com.shedule.x.config.factory;

import com.shedule.x.builder.FlatEdgeBuildingStrategy;
import com.shedule.x.builder.MetadataEdgeBuildingStrategy;
import com.shedule.x.builder.SymmetricEdgeBuildingStrategy;
import com.shedule.x.models.Node;
import com.shedule.x.processors.LSHIndex;
import com.shedule.x.processors.LSHIndexImpl;
import com.shedule.x.service.CompatibilityCalculator;
import com.shedule.x.processors.MetadataEncoder;
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
    private final CompatibilityCalculator compatibilityCalculator;
    private final MetadataEncoder encoder;
    private final Integer candidateLimit;
    private final Double similarityThreshold;
    private final ExecutorService executor;

    @Autowired
    public SymmetricEdgeBuildingStrategyFactory(
            FlatEdgeBuildingStrategy flatEdgeBuildingStrategy,
            ObjectProvider<LSHIndex> lshIndexProvider,
            CompatibilityCalculator compatibilityCalculator,
            MetadataEncoder encoder,
            Integer candidateLimit,
            Double similarityThreshold,
            @Qualifier("graphBuildExecutor") ExecutorService executor) {
        this.flatEdgeBuildingStrategy = flatEdgeBuildingStrategy;
        this.lshIndexProvider = lshIndexProvider;
        this.compatibilityCalculator = compatibilityCalculator;
        this.candidateLimit = candidateLimit;
        this.similarityThreshold = similarityThreshold;
        this.encoder = encoder;
        this.executor = executor;
    }

    public SymmetricEdgeBuildingStrategy createStrategy(String weightFunctionKey, List<Node> nodes) {
        Map<UUID, Node> nodeMap = nodes.stream()
                .collect(Collectors.toMap(Node::getId, node -> node));
        if ("flat".equalsIgnoreCase(weightFunctionKey)) {
            return flatEdgeBuildingStrategy;
        } else {
            return new MetadataEdgeBuildingStrategy(
                    lshIndexProvider.getObject(),
                    compatibilityCalculator,
                    encoder,
                    candidateLimit,
                    similarityThreshold,
                    executor
            );
        }
    }
}