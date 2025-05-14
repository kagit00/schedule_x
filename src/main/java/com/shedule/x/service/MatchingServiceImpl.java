package com.shedule.x.service;

import com.shedule.x.dto.MatchResult;
import com.shedule.x.dto.MatchingRequest;
import com.shedule.x.processors.GraphPreProcessor;
import com.shedule.x.processors.WeightFunctionResolver;
import com.shedule.x.matcher.strategies.decider.MatchingStrategySelector;
import com.shedule.x.models.*;
import com.shedule.x.matcher.strategies.MatchingStrategy;
import com.shedule.x.repo.NodeRepository;

import jakarta.transaction.Transactional;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;


@Service
@RequiredArgsConstructor
@Slf4j
@Data
public class MatchingServiceImpl implements MatchingService {
    private final NodeRepository nodeRepository;
    private final MatchingStrategySelector strategySelector;
    private final WeightFunctionResolver weightFunctionResolver;
    private final GraphPreProcessor graphPreProcessor;
    private int lastNodeCount;

    @Transactional
    public Map<String, List<MatchResult>> matchByGroup(MatchingRequest request, int page) {
        String groupId = request.getGroupId();
        LocalDateTime createdAfter = request.getCreatedAfter();

        int defaultLimit = request.isRealTime() ? 1000 : 10000;
        int limitValue = request.getLimit() != null ? Math.min(Math.max(request.getLimit(), 1), 50000) : defaultLimit;
        Pageable pageable = PageRequest.of(page, limitValue);

        List<Node> nodes;
        if (createdAfter != null) {
            log.debug("Fetching nodes for groupId={} after createdAt={}", groupId, createdAfter);
            nodes = nodeRepository.findFilteredByGroupIdAfter(groupId, createdAfter, pageable);
        } else {
            log.debug("Fetching all nodes for groupId={}", groupId);
            nodes = nodeRepository.findByGroupId(groupId, pageable);
        }

        lastNodeCount = nodes.size();
        log.debug("Fetched {} nodes for groupId={}: {}", lastNodeCount, groupId, nodes);
        if (lastNodeCount == limitValue) {
            log.warn("Node limit ({}) reached for groupId={}. Fetching next page.", limitValue, groupId);
        }

        String weightFunctionKey = weightFunctionResolver.resolveWeightFunctionKey(groupId);
        request.setWeightFunctionKey(weightFunctionKey);

        GraphBuilder.GraphResult graphResult = graphPreProcessor.buildGraph(nodes, request);
        log.debug("Graph built for groupId={}: {}", groupId, graphResult);

        MatchingStrategy strategy = strategySelector.select(
                MatchingContext.builder()
                        .sizeOfNodes(nodes.size()).domainId(request.getDomainId())
                        .industry(request.getIndustry())
                        .matchType(graphResult.graph().getType())
                        .isCostBased(!"flat".equalsIgnoreCase(weightFunctionKey))
                        .isRealTime(request.isRealTime())
                        .build()
        , groupId);

        Map<String, List<MatchResult>> results = strategy.match(graphResult, groupId, request.getDomainId());
        log.debug("Matches for groupId={}: {}", groupId, results);
        return results;
    }

    @Override
    public Map<String, List<MatchResult>> matchByGroup(MatchingRequest request) {
        return matchByGroup(request, 0);
    }
}