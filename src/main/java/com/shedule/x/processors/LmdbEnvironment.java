package com.shedule.x.processors;

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

    @Value("${graph.store.map-size:214748364800}")
    private long maxDbSize;

    @PostConstruct
    public void init() throws IOException {
        Path path = Paths.get(dbPath);
        Files.createDirectories(path);

        log.info("Initializing LMDB at {} (map size = {} GB)",
                path.toAbsolutePath(),
                maxDbSize / (1024L * 1024 * 1024));

        this.env = Env.create()
                .setMapSize(maxDbSize)
                .setMaxDbs(4)
                .setMaxReaders(512)
                .open(path.toFile(),
                        EnvFlags.MDB_WRITEMAP,
                        EnvFlags.MDB_MAPASYNC);

        this.nodeDbi = env.openDbi("nodes", DbiFlags.MDB_CREATE);
        this.edgeDbi = env.openDbi("edges",  DbiFlags.MDB_CREATE);
        this.lshDbi  = env.openDbi("lsh",    DbiFlags.MDB_CREATE);

        log.info("LMDB successfully initialized — ready for edges, LSH and nodes");
    }

    @PreDestroy
    @Override
    public void close() {
        if (env == null) return;

        try {
            log.warn("Graceful shutdown: syncing LMDB to disk...");
            env.sync(true);
            Thread.sleep(300);
        } catch (Exception e) {
            log.error("Error during LMDB sync on shutdown", e);
        } finally {
            if (edgeDbi != null) edgeDbi.close();
            if (lshDbi  != null) lshDbi.close();
            if (nodeDbi != null) nodeDbi.close();
            env.close();
            log.info("LMDB closed cleanly");
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
                return Files.size(Paths.get(dbPath, "data.mdb")) / (1024L * 1024 * 1024);
            } catch (Exception e) {
                return -1;
            }
        }
    }
}
