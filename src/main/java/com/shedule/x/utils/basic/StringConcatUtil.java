package com.shedule.x.utils.basic;

public class StringConcatUtil {

    private StringConcatUtil() {
        throw new UnsupportedOperationException("unsupported");
    }

    public static String concat(String... parts) {
        if (parts == null || parts.length == 0) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        for (String part : parts) {
            if (part != null) {
                builder.append(part);
            }
        }
        return builder.toString();
    }

    public static String concatWithSeparator(String separator, String... parts) {
        if (parts == null || parts.length == 0) {
            return "";
        }
        return String.join(separator != null ? separator : "", parts);
    }
}
