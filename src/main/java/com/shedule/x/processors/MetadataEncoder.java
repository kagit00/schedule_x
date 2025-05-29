package com.shedule.x.processors;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Component
public class MetadataEncoder {
    private final ConcurrentHashMap<String, Integer> idCache = new ConcurrentHashMap<>();
    private final AtomicInteger idCounter = new AtomicInteger();

    public int[] encode(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            log.warn("Null or empty metadata");
            return new int[0];
        }
        List<Integer> encoded = new ArrayList<>();
        List<String> sortedKeys = metadata.keySet().stream().sorted().toList();
        for (String key : sortedKeys) {
            String value = metadata.get(key);
            if (key != null && value != null) {
                String normalizedValue = value.trim().toLowerCase();
                int valueId = idCache.computeIfAbsent(normalizedValue, s -> idCounter.getAndIncrement());
                encoded.add(valueId);
                log.debug("Encoded key={} value={} normalized={} valueId={}", key, value, normalizedValue, valueId);
            }
        }
        int[] result = encoded.stream().mapToInt(Integer::intValue).toArray();
        log.debug("Encoded metadata: raw={}, result={}", metadata, Arrays.toString(result));
        return result;
    }

    public List<Map.Entry<int[], UUID>> encodeBatch(List<Map.Entry<Map<String, String>, UUID>> entries) {
        idCache.clear();
        idCounter.set(0);
        log.info("Cleared idCache for batch encoding");
        return entries.stream()
                .map(entry -> {
                    UUID nodeId = entry.getValue();
                    if (nodeId == null) {
                        log.warn("Null nodeId for metadata: {}", entry.getKey());
                        return null;
                    }
                    int[] encoded = encode(entry.getKey());
                    return new AbstractMap.SimpleEntry<>(encoded, nodeId);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}