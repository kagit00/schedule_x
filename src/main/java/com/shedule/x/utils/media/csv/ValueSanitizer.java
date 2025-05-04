package com.shedule.x.utils.media.csv;

public final class ValueSanitizer {

    private ValueSanitizer() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public static String sanitize(String value) {
        if (value == null) {
            return "";
        }

        String sanitized = value.replace("\n", " ").replace("\r", " ").trim();

        if (sanitized.startsWith("\"") && sanitized.endsWith("\"") && sanitized.length() > 1) {
            sanitized = sanitized.substring(1, sanitized.length() - 1);
        }

        return sanitized;
    }
}