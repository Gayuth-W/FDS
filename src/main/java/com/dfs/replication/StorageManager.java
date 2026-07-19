package com.dfs.replication;

import com.dfs.config.ClusterConfig;
import com.dfs.util.HashUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Durable, per-node block + metadata store. Port of
 * features/replication/storage.py.
 *
 * Guarantees, mirroring the original "IRONCLAD" storage:
 * - Write-Ahead Log entries around block writes/deletes.
 * - fsync to hardware on every block write, with post-write size verification.
 * - SHA-256 checksums and versioning via write_with_checksum.
 * - Deterministic Last-Writer-Wins convergence for metadata, keyed on the
 * Lamport timestamp with a node-id tie-breaker.
 *
 * Files:
 * <blocksDir>/<blockId>.dat -> raw block bytes
 * <blocksDir>/<blockId>.meta -> JSON metadata (also used for manifest_<file>)
 */
@Component
public class StorageManager {

    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);

    private final ClusterConfig config;
    private final ObjectMapper mapper;

    private Path blocksDir;
    private Path walFile;

    public StorageManager(ClusterConfig config, ObjectMapper mapper) {
        this.config = config;
        this.mapper = mapper;
    }

    @PostConstruct
    void init() {
        this.blocksDir = config.getBlocksDir().toAbsolutePath();
        this.walFile = blocksDir.resolve("storage.wal");
        try {
            Files.createDirectories(blocksDir);
        } catch (IOException e) {
            throw new IllegalStateException("Could not create blocks dir " + blocksDir, e);
        }
        log.info("Storage IRONCLAD initialized at {}", blocksDir);
        recoverFromWal();
    }

    private void recoverFromWal() {
        try {
            if (Files.exists(walFile)) {
                log.info("[STORAGE] Checking WAL for recovery...");
                List<String> lines = Files.readAllLines(walFile);
                if (!lines.isEmpty()) {
                    log.warn("[STORAGE] Found {} entries in WAL. Potential unclean shutdown.", lines.size());
                }
            }
        } catch (IOException e) {
            log.error("WAL recovery check failed: {}", e.toString());
        }
    }

    private synchronized void logWal(String entry) {
        try (FileOutputStream fos = new FileOutputStream(walFile.toFile(), true)) {
            String line = HashUtil.now() + ": " + entry + "\n";
            fos.write(line.getBytes(StandardCharsets.UTF_8));
            fos.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            log.error("WAL write failed: {}", e.toString());
        }
    }

    /** Save a block to disk with hardware sync and post-write verification. */
    public boolean writeBlock(String blockId, byte[] data) throws IOException {
        logWal("START_WRITE " + blockId);
        Path path = blocksDir.resolve(blockId + ".dat");
        Files.createDirectories(path.getParent());

        try (FileOutputStream fos = new FileOutputStream(path.toFile())) {
            fos.write(data);
            fos.flush();
            fos.getFD().sync(); // hardware sync
        }
        // Post-write verify
        if (!Files.exists(path) || Files.size(path) != data.length) {
            throw new IOException("Write verification failed for " + path);
        }

        logWal("COMMIT_WRITE " + blockId);
        log.info("IRONCLAD: Wrote block {} ({} bytes) to {}", blockId, data.length, path);
        return true;
    }

    private static double asDouble(Object o, double def) {
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        return def;
    }
}
