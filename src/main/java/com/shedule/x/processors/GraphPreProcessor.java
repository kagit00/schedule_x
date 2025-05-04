package com.shedule.x.processors;

import com.shedule.x.dto.enums.MatchType;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.dto.enums.NodeType;
import com.shedule.x.exceptions.InternalServerErrorException;

import com.shedule.x.models.Node;
import com.shedule.x.partition.MetadataBasedPartitioningStrategy;
import com.shedule.x.partition.PartitionStrategy;
import com.shedule.x.service.GraphBuilder;
import com.shedule.x.service.GraphBuilderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class GraphPreProcessor {

    private final GraphBuilderService graphBuilder;

    public MatchType inferMatchType(List<Node> firstNodeList, List<Node> secondNodeList) {
        if (firstNodeList == null || secondNodeList == null || firstNodeList.isEmpty() || secondNodeList.isEmpty()) {
            return MatchType.SYMMETRIC;
        }
        Set<NodeType> firstTypes = firstNodeList.stream().map(Node::getType).collect(Collectors.toSet());
        Set<NodeType> secondTypes = secondNodeList.stream().map(Node::getType).collect(Collectors.toSet());
        return firstTypes.equals(secondTypes) ? MatchType.SYMMETRIC : MatchType.BIPARTITE;
    }

    public GraphBuilder.GraphResult buildGraph(List<Node> nodes, MatchingRequest request) {
        String key = request.getPartitionKey();
        String leftVal = request.getLeftPartitionValue();
        String rightVal = request.getRightPartitionValue();

        boolean isPartitioningApplicable = !StringUtils.isEmpty(key) &&
                !StringUtils.isEmpty(leftVal) &&
                !StringUtils.isEmpty(rightVal);

        PartitionStrategy strategy = isPartitioningApplicable
                ? new MetadataBasedPartitioningStrategy(key, leftVal, rightVal)
                : null;

        MatchType matchType = Optional.ofNullable(request.getMatchType()).orElse(MatchType.AUTO);

        return switch (matchType) {
            case SYMMETRIC -> graphBuilder.buildSymmetric(nodes, request.getWeightFunctionKey(), request.getGroupId());

            case BIPARTITE -> {
                if (strategy == null) throw new InternalServerErrorException("Partition strategy required for BIPARTITE matching.");
                Pair<List<Node>, List<Node>> parts = strategy.partition(nodes);
                log.info("Partitioned: left={}, right={}", parts.getLeft().size(), parts.getRight().size());
                yield graphBuilder.build(parts.getLeft(), parts.getRight(), request.getWeightFunctionKey());
            }

            case AUTO -> {
                if (!isPartitioningApplicable) {
                    log.info("No partitioning keys detected; defaulting to SYMMETRIC match.");
                    yield graphBuilder.buildSymmetric(nodes, request.getWeightFunctionKey(), request.getGroupId());
                }

                Pair<List<Node>, List<Node>> parts = strategy.partition(nodes);
                MatchType inferred = inferMatchType(parts.getLeft(), parts.getRight());

                log.info("Auto-inferred match type: {}", inferred);
                yield inferred == MatchType.SYMMETRIC
                        ? graphBuilder.buildSymmetric(nodes, request.getWeightFunctionKey(), request.getGroupId())
                        : graphBuilder.build(parts.getLeft(), parts.getRight(), request.getWeightFunctionKey());
            }
        };
    }
}