package com.shedule.x.processors;

import com.shedule.x.utils.basic.Murmur3;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

import java.util.stream.Collectors;

import java.util.*;

@Component
@Slf4j
public class MetadataEncoder {


    public int[] encode(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return new int[0];
        }

        // 1. Stream entries and sort by key for deterministic vector position
        return metadata.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .mapToInt(entry -> {
                    // Combine key and value (e.g., "city:boston")
                    String feature = entry.getKey() + ":" + entry.getValue();
                    // Use Murmur3 32-bit hash for the feature ID
                    return Murmur3.hash32(feature);
                })
                .toArray();
    }

    /**
     * Encodes a batch of nodes for LSH indexing.
     */
    public List<Map.Entry<int[], UUID>> encodeBatch(List<Map.Entry<Map<String, String>, UUID>> entries) {
        // We can keep parallelStream here as it's now CPU-bound and not hitting a shared, locked resource.
        return entries.parallelStream()
                .map(entry -> {
                    int[] encoded = encode(entry.getKey());
                    return new AbstractMap.SimpleEntry<>(encoded, entry.getValue());
                })
                .collect(Collectors.toList());
    }
}