package com.shedule.x.config.factory;

import com.shedule.x.config.QueueConfig;
import com.shedule.x.processors.PotentialMatchComputationProcessor;
import com.shedule.x.processors.QueueManagerImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Lazy;


@Getter
@Component
public class QueueManagerFactory {
    private final Path spillBaseDir;
    private final MeterRegistry meterRegistry;
    private final ScheduledExecutorService flushScheduler;
    private final PotentialMatchComputationProcessor processor;

    public QueueManagerFactory(
            @Value("${match.spill.dir:/tmp/spill}") String spillDir,
            MeterRegistry meterRegistry,
            @Qualifier("queueFlushScheduler") ScheduledExecutorService flushScheduler,
            @Lazy PotentialMatchComputationProcessor processor) throws IOException { // <--- FIX

        this.spillBaseDir = Paths.get(spillDir);
        Files.createDirectories(this.spillBaseDir);
        this.meterRegistry = meterRegistry;
        this.flushScheduler = flushScheduler;
        this.processor = processor;
    }

    public QueueManagerImpl create(QueueConfig config) {
        return QueueManagerImpl.getOrCreate(
                config,
                meterRegistry,
                flushScheduler,
                (gid, did, size, cycleId) -> processor.savePendingMatchesAsync(gid, did, cycleId, size),
                spillBaseDir
        );
    }

    public QuadFunction<UUID, UUID, Integer, String, CompletableFuture<Void>> getFlushSignalCallback() {
        return (gid, did, size, cycleId) -> processor.savePendingMatchesAsync(gid, did, cycleId, size);
    }
}