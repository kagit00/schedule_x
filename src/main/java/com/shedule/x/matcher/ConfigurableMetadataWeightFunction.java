package com.shedule.x.matcher;

import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import java.util.Map;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@RequiredArgsConstructor
public class ConfigurableMetadataWeightFunction implements BiFunction<Map<String, String>, Map<String, String>, Double> {

    private final Set<String> headers;

    @Override
    public Double apply(Map<String, String> left, Map<String, String> right) {
        try {
            Map<String, String> l = normalizeMetadata(left);
            Map<String, String> r = normalizeMetadata(right);
            Map<String, Integer> weights = extractKeyWeights(l, r);

            double score = 0.0;
            int total = 0;

            for (String key : headers) {
                int weight = weights.getOrDefault(key, 1);
                total += weight;

                if (key.equals("preferences")) {
                    score += computePreferenceMatchScore(l, r, weight);
                } else if (key.endsWith("_list")) {
                    score += computeListMatchScore(l, r, key, weight);
                } else if (l.getOrDefault(key, "").equals(r.get(key))) {
                    score += weight;
                }
            }

            return total > 0 ? score / total : 0.0;
        } catch (Exception e) {
            return 0.0;
        }
    }

    private Map<String, String> normalizeMetadata(Map<String, String> map) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toLowerCase().trim(),
                        e -> e.getValue() != null ? e.getValue().toLowerCase().trim() : ""
                ));
    }

    private Map<String, Integer> extractKeyWeights(Map<String, String> a, Map<String, String> b) {
        Map<String, Integer> weights = new HashMap<>();
        Stream.of(a, b).forEach(map ->
                map.forEach((k, v) -> {
                    if (k.startsWith("weight_")) {
                        try {
                            weights.putIfAbsent(k.substring(7), Integer.parseInt(v));
                        } catch (NumberFormatException ignored) {}
                    }
                }));
        return weights;
    }

    private double computeListMatchScore(Map<String, String> l, Map<String, String> r, String key, int weight) {
        String[] leftArr = l.getOrDefault(key, "").split(",");
        String[] rightArr = r.getOrDefault(key, "").split(",");

        if (leftArr.length == 0 || rightArr.length == 0) return 0.0;

        Set<String> leftSet = Arrays.stream(leftArr).map(String::trim).collect(Collectors.toSet());
        Set<String> rightSet = Arrays.stream(rightArr).map(String::trim).collect(Collectors.toSet());

        leftSet.retainAll(rightSet);
        return weight * ((double) leftSet.size() / Math.max(leftArr.length, rightArr.length));
    }

    private double computePreferenceMatchScore(Map<String, String> l, Map<String, String> r, int weight) {
        String prefs = l.getOrDefault("preferences", "");
        if (prefs.isBlank()) return 0.0;

        String[] prefArray = prefs.split(",");
        long matches = Arrays.stream(prefArray)
                .map(String::trim)
                .filter(p -> {
                    String[] kv = p.split(":");
                    return kv.length == 2 && kv[1].equalsIgnoreCase(r.getOrDefault(kv[0], ""));
                })
                .count();

        return weight * ((double) matches / prefArray.length);
    }
}
