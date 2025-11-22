package com.shedule.x.processors;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;


@Component
@Slf4j
public class LmdbEnvironment implements AutoCloseable {
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> edgeDbi;
    private Dbi<ByteBuffer> lshDbi;

    @Value("${graph.store.path:/app/graph-store}")
    private String dbPath;

    // Default 512GB max map size (virtual memory, doesn't use RAM unless needed)
    @Value("${graph.store.map-size:549755813888}")
    private long maxDbSize;

    @PostConstruct
    public void init() throws IOException {
        Path path = Path.of(dbPath);
        Files.createDirectories(path);

        this.env = Env.create()
                .setMapSize(maxDbSize)
                .setMaxDbs(4)
                .setMaxReaders(1024) // Reduced from 4096 to save OS lock files
                .open(path.toFile(), EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_MAPASYNC, EnvFlags.MDB_NOSYNC);

        this.edgeDbi = env.openDbi("edges", DbiFlags.MDB_CREATE);
        this.lshDbi = env.openDbi("lsh", DbiFlags.MDB_CREATE);

        log.info("LMDB initialized at {}", path.toAbsolutePath());
    }

    @Override
    public void close() {
        if (edgeDbi != null) edgeDbi.close();
        if (lshDbi != null) lshDbi.close();
        if (env != null) {
            // Force sync before closing to persist NOSYNC data
            try { env.sync(true); } catch (Exception e) { log.warn("Sync failed", e); }
            env.close();
        }
    }

    public Env<ByteBuffer> env() { return env; }
    public Dbi<ByteBuffer> edgeDbi() { return edgeDbi; }
    public Dbi<ByteBuffer> lshDbi() { return lshDbi; }
}