package com.shedule.x.processors;

import com.shedule.x.models.Node;
import com.shedule.x.service.CompatibilityCalculator;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public class MetadataCompatibilityCalculator implements CompatibilityCalculator {
    private static final double DEFAULT_SCORE = 0.4;
    private static final double MATCH_SCORE = 0.7;
    private static final double NUMERIC_TOLERANCE = 0.6;
    private final ConcurrentHashMap<String, Set<String>> keyCache = new ConcurrentHashMap<>();

    @Override
    public double calculate(Node node1, Node node2) {
        Map<String, String> meta1 = node1.getMetaData();
        Map<String, String> meta2 = node2.getMetaData();

        if (meta1.isEmpty() || meta2.isEmpty()) {
            log.debug("Empty metadata for nodes {} or {}; using default score", node1.getId(), node2.getId());
            return DEFAULT_SCORE;
        }

        String cacheKey = node1.getId() + ":" + node2.getId();
        Set<String> commonKeys = keyCache.computeIfAbsent(cacheKey, k -> getCommonKeys(meta1, meta2));

        if (commonKeys.isEmpty()) {
            log.debug("No common metadata keys for nodes {} and {}", node1.getId(), node2.getId());
            return DEFAULT_SCORE;
        }

        double score = commonKeys.stream()
                .mapToDouble(key -> scoreForKey(meta1.get(key), meta2.get(key)))
                .sum();

        return Math.max(score, DEFAULT_SCORE);
    }

    private Set<String> getCommonKeys(Map<String, String> meta1, Map<String, String> meta2) {
        Set<String> keys = new HashSet<>(meta1.keySet());
        keys.retainAll(meta2.keySet());
        return keys;
    }

    private double scoreForKey(String rawValue1, String rawValue2) {
        if (rawValue1 == null || rawValue2 == null) return 0.0;

        String value1 = rawValue1.trim().toLowerCase();
        String value2 = rawValue2.trim().toLowerCase();

        if (value1.equals(value2)) return MATCH_SCORE;
        if (isNumericMatch(value1, value2)) return MATCH_SCORE;
        if (isMultiValueMatch(value1, value2)) return MATCH_SCORE;
        if (value1.contains(value2) || value2.contains(value1)) return MATCH_SCORE * 0.5;

        return 0.0;
    }

    private boolean isNumericMatch(String value1, String value2) {
        try {
            double num1 = Double.parseDouble(value1);
            double num2 = Double.parseDouble(value2);
            return Math.abs(num1 - num2) / Math.max(Math.abs(num1), 1.0) <= NUMERIC_TOLERANCE;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isMultiValueMatch(String value1, String value2) {
        if (!value1.contains(",") && !value2.contains(",")) return false;

        Set<String> set1 = new HashSet<>(Arrays.asList(value1.split("\\s*,\\s*")));
        Set<String> set2 = new HashSet<>(Arrays.asList(value2.split("\\s*,\\s*")));
        set1.remove("");
        set2.remove("");

        return !set1.isEmpty() && !set2.isEmpty() && !Collections.disjoint(set1, set2);
    }
}