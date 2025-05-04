package com.shedule.x.processors;

import com.shedule.x.matcher.ConfigurableMetadataWeightFunction;
import com.shedule.x.repo.NodeRepository;
import com.shedule.x.utils.graph.WeightFunctionRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class WeightFunctionResolver {

    private final NodeRepository nodeRepository;

    public String resolveWeightFunctionKey(String groupId) {
        Set<String> headers = nodeRepository.findDistinctMetadataKeysByGroupId(groupId);
        if (headers == null || headers.isEmpty()) {
            log.warn("No metadata keys found for groupId={}. Defaulting to 'flat'", groupId);
            return "flat";
        }

        Set<String> validHeaders = headers.stream()
                .filter(key -> key != null && !key.isBlank() && !key.startsWith("temp_"))
                .limit(10)
                .collect(Collectors.toSet());

        if (validHeaders.isEmpty()) {
            log.warn("No valid metadata keys for groupId={}. Defaulting to 'flat'", groupId);
            return "flat";
        }

        String weightKey = groupId + "-" + validHeaders.stream()
                .map(String::toLowerCase)
                .sorted()
                .collect(Collectors.joining("-"));

        if (!WeightFunctionRegistry.getRegistry().containsKey(weightKey)) {
            WeightFunctionRegistry.registerIfAbsent(weightKey, new ConfigurableMetadataWeightFunction(validHeaders));
            log.info("Registered weightFunctionKey={} for groupId={}", weightKey, groupId);
        }

        return weightKey;
    }
}