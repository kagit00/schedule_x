package com.shedule.x.utils.db;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@UtilityClass
@Slf4j
public final class BatchUtils {
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

    public static  <T> List<Stream<T>> partition(Stream<T> stream, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        List<T> currentBatch = new ArrayList<>();
        int[] count = {0};

        stream.forEach(item -> {
            currentBatch.add(item);
            count[0]++;
            if (currentBatch.size() >= batchSize) {
                batches.add(new ArrayList<>(currentBatch));
                currentBatch.clear();
            }
        });

        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        log.debug("Partitioned stream into {} chunks of size up to {}", batches.size(), batchSize);
        return batches.stream().map(List::stream).collect(Collectors.toList());
    }
}