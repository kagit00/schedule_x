package com.shedule.x.utils.media.csv;

public final class RowValidator {

    private RowValidator() {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    public static boolean isValid(String referenceId, String groupId) {
        return referenceId != null && !referenceId.isEmpty() &&
               groupId != null && !groupId.isEmpty();
    }
}