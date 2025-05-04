package com.shedule.x.utils.db;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class BatchUtils {

    private BatchUtils() {

    }

    public static <T> void processInBatches(List<T> items, int batchSize, Consumer<List<T>> consumer) {
        for (int i = 0; i < items.size(); i += batchSize) {
            int end = Math.min(i + batchSize, items.size());
            List<T> batch = items.subList(i, end);
            consumer.accept(batch);
        }
    }

    public static <T> List<List<T>> partition(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            batches.add(list.subList(i, Math.min(i + batchSize, list.size())));
        }
        return batches;
    }
}