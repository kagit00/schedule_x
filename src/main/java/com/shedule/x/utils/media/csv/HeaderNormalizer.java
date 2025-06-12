package com.shedule.x.utils.media.csv;

import java.util.HashMap;
import java.util.Map;

public final class HeaderNormalizer {

    private HeaderNormalizer() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public static final String FIELD_TYPE = "type";
    public static final String FIELD_REFERENCE_ID = "reference_id";
    public static final String FIELD_GROUP_ID = "group_id";

    public static Map<String, Integer> normalizeHeaders(String[] headers) {
        Map<String, Integer> headerMap = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i] != null ? headers[i].trim() : "";
            if (!header.isEmpty()) {
                String normalized = normalize(header);
                headerMap.put(normalized, i);
            }
        }
        return headerMap;
    }

    private static String normalize(String header) {
        String normalized = header.toLowerCase().replaceAll("[^a-z0-9_]", "_");
        return switch (normalized) {
            case "referenceid", "refid", "ref_id", "ref", "reference", "id", "userid", "user_id" -> FIELD_REFERENCE_ID;
            case "groupid", "group_id", "group", "grp_id", "grp" -> FIELD_GROUP_ID;
            case "nodetype", "node_type", "type" -> FIELD_TYPE;
            default -> normalized;
        };
    }
}