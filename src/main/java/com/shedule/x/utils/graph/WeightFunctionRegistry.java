package com.shedule.x.utils.graph;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class WeightFunctionRegistry {

    private static final Map<String, BiFunction<Map<String, String>, Map<String, String>, Double>> registry = new ConcurrentHashMap<>();

    static {
        registry.put("flat", (left, right) -> 1.0);

        registry.put("generic_similarity", (left, right) -> {
            try {
                Map<String, String> normLeft = normalizeMetadata(left);
                Map<String, String> normRight = normalizeMetadata(right);
                long matchingCount = normLeft.entrySet().stream()
                        .filter(entry -> normRight.containsKey(entry.getKey()) &&
                                entry.getValue().equals(normRight.get(entry.getKey())))
                        .count();
                int totalKeys = Math.max(normLeft.size(), normRight.size());
                return totalKeys > 0 ? (double) matchingCount / totalKeys : 0.0;
            } catch (Exception e) {
                return 0.0;
            }
        });

        registry.put("weighted_similarity", (left, right) -> {
            try {
                Map<String, String> normLeft = normalizeMetadata(left);
                Map<String, String> normRight = normalizeMetadata(right);
                double score = 0.0;
                int totalWeight = 0;
                // Dynamic weights from metadata (e.g., "weight_keyname=3")
                Map<String, Integer> keyWeights = extractKeyWeights(normLeft, normRight);
                for (Map.Entry<String, String> entry : normLeft.entrySet()) {
                    String key = entry.getKey();
                    int weight = keyWeights.getOrDefault(key, 1);
                    if (normRight.containsKey(key) && entry.getValue().equals(normRight.get(key))) {
                        score += weight;
                    }
                    totalWeight += weight;
                }
                return totalWeight > 0 ? score / totalWeight : 0.0;
            } catch (Exception e) {
                return 0.0;
            }
        });

        registry.put("preference_similarity", (left, right) -> {
            try {
                Map<String, String> normLeft = normalizeMetadata(left);
                Map<String, String> normRight = normalizeMetadata(right);
                double score = 0.0;

                long matchingCount = normLeft.entrySet().stream()
                        .filter(entry -> normRight.containsKey(entry.getKey()) &&
                                entry.getValue().equals(normRight.get(entry.getKey())))
                        .count();
                int totalKeys = Math.max(normLeft.size(), normRight.size());
                score += totalKeys > 0 ? 0.5 * ((double) matchingCount / totalKeys) : 0.0;

                String leftPrefs = normLeft.getOrDefault("preferences", "");
                if (!leftPrefs.isEmpty()) {
                    String[] prefArray = leftPrefs.split(",");
                    long prefMatches = Arrays.stream(prefArray)
                            .filter(pref -> {
                                String[] parts = pref.split(":");
                                return parts.length == 2 && normRight.containsKey(parts[0]) &&
                                        normRight.get(parts[0]).equals(parts[1]);
                            })
                            .count();
                    score += prefMatches > 0 ? 0.5 * ((double) prefMatches / prefArray.length) : 0.0;
                }
                return score;
            } catch (Exception e) {
                return 0.0;
            }
        });
    }

    private static Map<String, String> normalizeMetadata(Map<String, String> metaData) {
        return metaData.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().toLowerCase().trim(),
                        entry -> entry.getValue() != null ? entry.getValue().toLowerCase().trim() : ""
                ));
    }

    private static Map<String, Integer> extractKeyWeights(Map<String, String> left, Map<String, String> right) {
        Map<String, Integer> weights = new ConcurrentHashMap<>();
        left.forEach((key, value) -> {
            if (key.startsWith("weight_")) {
                try {
                    String targetKey = key.substring(7);
                    weights.put(targetKey, Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    // Ignore invalid weights
                }
            }
        });
        right.forEach((key, value) -> {
            if (key.startsWith("weight_")) {
                try {
                    String targetKey = key.substring(7);
                    weights.putIfAbsent(targetKey, Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    // Ignore invalid weights
                }
            }
        });
        return weights;
    }

    private WeightFunctionRegistry() {
        throw new UnsupportedOperationException("No instantiation allowed.");
    }

    public static BiFunction<Map<String, String>, Map<String, String>, Double> get(String key) {
        return registry.getOrDefault(key, registry.get("flat"));
    }

    public static void register(String key, BiFunction<Map<String, String>, Map<String, String>, Double> function) {
        registry.put(key, function);
    }

    public static void registerIfAbsent(String key, BiFunction<Map<String, String>, Map<String, String>, Double> function) {
        registry.putIfAbsent(key, function);
    }

    public static Map<String, BiFunction<Map<String, String>, Map<String, String>, Double>> getRegistry() {
        return registry;
    }
}