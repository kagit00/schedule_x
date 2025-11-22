package com.shedule.x.config.factory;

import com.shedule.x.models.Edge;
import lombok.Getter;

import java.util.function.Consumer;
import java.util.stream.Stream;

@Getter
public final class AutoCloseableStream<T> implements AutoCloseable {
    private final Stream<T> stream;
    private final Runnable onClose;

    public AutoCloseableStream(Stream<T> stream, Runnable onClose) {
        this.stream = (stream != null ? stream : Stream.<T>empty())
                .onClose(onClose);
        this.onClose = onClose;
    }

    public AutoCloseableStream(Stream<T> stream) {
        this(stream, () -> {});
    }

    public static <T> AutoCloseableStream<T> of(Stream<T> stream) {
        return new AutoCloseableStream<>(stream);
    }

    public static <T> AutoCloseableStream<T> empty() {
        return new AutoCloseableStream<>(Stream.empty());
    }

    public void forEach(Consumer<? super T> action) {
        stream.forEach(action);
    }

    public Stream<T> stream() {
        return stream;
    }

    @Override
    public void close() {
        try { stream.close(); }
        finally { onClose.run(); }
    }
}
