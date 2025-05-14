package com.shedule.x.utils.media.csv;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ValueSanitizer {

    private ValueSanitizer() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public static String sanitize(String value) {
        long startTime = System.nanoTime();
        if (value == null || value.isEmpty()) {
            return "";
        }

        int length = value.length();
        boolean isQuoted = length > 1 && value.charAt(0) == '"' && value.charAt(length - 1) == '"';
        StringBuilder sanitized = getStringBuilder(value, isQuoted, length);

        String result = sanitized.toString();
        log.debug("Sanitized string in {} µs", (System.nanoTime() - startTime) / 1000);
        return result;
    }

    private static StringBuilder getStringBuilder(String value, boolean isQuoted, int length) {
        int start = isQuoted ? 1 : 0;
        int end = isQuoted ? length - 1 : length;

        while (start < end && (value.charAt(start) == ' ' || value.charAt(start) == '\t')) {
            start++;
        }
        while (end > start && (value.charAt(end - 1) == ' ' || value.charAt(end - 1) == '\t')) {
            end--;
        }

        StringBuilder sanitized = new StringBuilder(end - start);
        for (int i = start; i < end; i++) {
            char c = value.charAt(i);
            sanitized.append((c == '\n' || c == '\r') ? ' ' : c);
        }
        return sanitized;
    }

}
