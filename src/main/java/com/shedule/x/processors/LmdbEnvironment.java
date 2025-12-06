package com.shedule.x.processors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import jakarta.annotation.PreDestroy;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.lmdbjava.*;


@Slf4j
@Configuration
public class LmdbEnvironment implements AutoCloseable {

    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> edgeDbi;
    private Dbi<ByteBuffer> lshDbi;
    private Dbi<ByteBuffer> nodeDbi;

    @Value("${graph.store.path:/app/graph-store}")
    private String dbPath;

    @Value("${graph.store.map-size:549755813888}")
    private long maxDbSize;

    @Value("${graph.store.sync-interval-ms:1000}")
    private long syncIntervalMs;

    private final ScheduledExecutorService syncExecutor =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("lmdb-sync-%d")
                            .build()
            );

    @PostConstruct
    public void init() throws IOException {
        Path path = Paths.get(dbPath);
        Files.createDirectories(path);

        log.info("Initializing LMDB at {} ({} GiB)",
                path.toAbsolutePath(),
                maxDbSize / (1024L * 1024 * 1024));

        this.env = Env.create()
                .setMapSize(maxDbSize)
                .setMaxDbs(8)
                .setMaxReaders(2048)
                .open(path.toFile(),
                        EnvFlags.MDB_NOTLS
                );

        this.nodeDbi = env.openDbi("nodes", DbiFlags.MDB_CREATE);
        this.edgeDbi = env.openDbi("edges", DbiFlags.MDB_CREATE);
        this.lshDbi  = env.openDbi("lsh",   DbiFlags.MDB_CREATE);

        startPeriodicSync();

        log.info("LMDB initialized successfully. Database path: {}", path);
    }

    private void startPeriodicSync() {
        syncExecutor.scheduleAtFixedRate(() -> {
            try {
                env.sync(false);
                log.debug("LMDB synced to disk");
            } catch (Exception e) {
                log.error("Failed to sync LMDB", e);
            }
        }, syncIntervalMs, syncIntervalMs, TimeUnit.MILLISECONDS);

        log.info("Periodic sync enabled (interval = {} ms)", syncIntervalMs);
    }

    @PreDestroy
    @Override
    public void close() {
        if (env == null) return;

        try {
            log.warn("Graceful shutdown: syncing LMDB to disk...");

            // Stop periodic sync
            syncExecutor.shutdown();
            syncExecutor.awaitTermination(5, TimeUnit.SECONDS);

            // Final sync
            env.sync(true); // Full sync

            log.info("LMDB synced and closed cleanly");
        } catch (Exception e) {
            log.error("Error during LMDB shutdown", e);
        } finally {
            try {
                if (edgeDbi != null) edgeDbi.close();
                if (lshDbi  != null) lshDbi.close();
                if (nodeDbi != null) nodeDbi.close();
                env.close();
            } catch (Exception e) {
                log.error("Error closing LMDB resources", e);
            }
        }
    }

    public Env<ByteBuffer> env()     { return env; }
    public Dbi<ByteBuffer> edgeDbi() { return edgeDbi; }
    public Dbi<ByteBuffer> lshDbi()  { return lshDbi; }
    public Dbi<ByteBuffer> nodeDbi() { return nodeDbi; }

    @Bean
    public LmdbStats lmdbStats() {
        return new LmdbStats();
    }

    public class LmdbStats {
        public long edgeCount() {
            if (env == null || edgeDbi == null) return 0;
            try (var txn = env.txnRead()) {
                return edgeDbi.stat(txn).entries;
            } catch (Exception e) {
                log.warn("Failed to read LMDB edge count", e);
                return -1;
            }
        }

        public long totalSizeGb() {
            try {
                Path dataFile = Paths.get(dbPath, "data.mdb");
                if (!Files.exists(dataFile)) return 0;
                return Files.size(dataFile) / (1024L * 1024 * 1024);
            } catch (Exception e) {
                log.warn("Failed to read LMDB size", e);
                return -1;
            }
        }

        public Map<String, Object> getStats() {
            Map<String, Object> stats = new HashMap<>();
            stats.put("edgeCount", edgeCount());
            stats.put("totalSizeGb", totalSizeGb());
            stats.put("path", dbPath);
            return stats;
        }
    }
}