package com.shedule.x.config.factory;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.config.QueueManagerConfig;
import com.shedule.x.processors.QueueManagerImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Getter;

@Getter
public class QueueManagerFactory {
    private final MeterRegistry meterRegistry;
    private final ExecutorService mappingExecutor;
    private final ExecutorService flushExecutor;
    private final QueueManagerImpl.QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback;
    private final ScheduledExecutorService flushScheduler;

    public QueueManagerFactory(
            MeterRegistry meterRegistry,
            @Qualifier("persistenceExecutor") ExecutorService mappingExecutor,
            @Qualifier("queueFlushExecutor") ExecutorService flushExecutor,
            @Qualifier("queueFlushScheduler") ScheduledExecutorService flushScheduler,
            QueueManagerImpl.QuadFunction<String, UUID, Integer, String, CompletableFuture<Void>> flushSignalCallback
    ) {
        this.meterRegistry = meterRegistry;
        this.mappingExecutor = mappingExecutor;
        this.flushExecutor = flushExecutor;
        this.flushScheduler = flushScheduler;
        this.flushSignalCallback = flushSignalCallback;
    }

    public QueueManagerImpl create(QueueConfig config) {
        return QueueManagerImpl.getOrCreate(
                config, meterRegistry,
                mappingExecutor, flushExecutor, flushScheduler, flushSignalCallback
        );
    }
}