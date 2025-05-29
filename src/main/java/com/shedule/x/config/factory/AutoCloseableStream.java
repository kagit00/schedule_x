package com.shedule.x.config.factory;

import lombok.Getter;

import java.util.function.Consumer;
import java.util.stream.Stream;

import java.util.function.Consumer;
import java.util.stream.Stream;

@Getter
public class AutoCloseableStream<T> implements AutoCloseable {
    private final Stream<T> stream;

    public AutoCloseableStream(Stream<T> stream) {
        this.stream = stream;
    }

    @Override
    public void close() {
        stream.close();
    }

    public void forEach(Consumer<? super T> action) {
        stream.forEach(action);
    }
}
