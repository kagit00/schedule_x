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
        switch (normalized) {
            case "referenceid":
            case "refid":
            case "ref_id":
            case "ref":
            case "reference":
            case "id":
            case "userid":
            case "user_id":
                return FIELD_REFERENCE_ID;
            case "groupid":
            case "group_id":
            case "group":
            case "grp_id":
            case "grp":
                return FIELD_GROUP_ID;
            case "nodetype":
            case "node_type":
            case "type":
                return FIELD_TYPE;
            default:
                return normalized;
        }
    }
}