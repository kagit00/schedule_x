package com.shedule.x.config.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.*;
import java.util.stream.Collector;

public class BatchCollector<T> implements Collector<T, List<T>, Void> {
    private final int batchSize;
    private final Consumer<List<T>> batchConsumer;

    public BatchCollector(int batchSize, Consumer<List<T>> batchConsumer) {
        this.batchSize = batchSize;
        this.batchConsumer = batchConsumer;
    }

    @Override public Supplier<List<T>> supplier() { return ArrayList::new; }
    @Override public BiConsumer<List<T>, T> accumulator() {
        return (list, item) -> {
            list.add(item);
            if (list.size() >= batchSize) {
                batchConsumer.accept(new ArrayList<>(list));
                list.clear();
            }
        };
    }
    @Override public BinaryOperator<List<T>> combiner() {
        return (a, b) -> { a.addAll(b); return a; };
    }
    @Override public Function<List<T>, Void> finisher() {
        return list -> {
            if (!list.isEmpty()) batchConsumer.accept(list);
            return null;
        };
    }
    @Override public Set<Characteristics> characteristics() {
        return Set.of();
    }
}