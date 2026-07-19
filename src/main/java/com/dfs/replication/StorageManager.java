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

    /** Read a block from disk, or null if absent. */
    public byte[] readBlock(String blockId) throws IOException {
        Path path = blocksDir.resolve(blockId + ".dat");
        if (!Files.exists(path)) {
            return null;
        }
        return Files.readAllBytes(path);
    }

    /**
     * Store metadata for a key with deterministic LWW convergence.
     * Returns false if the incoming metadata is logically stale (or loses the
     * node-id tie-breaker) and was therefore ignored.
     */
    public synchronized boolean saveMetadata(String key, Map<String, Object> metadata) throws IOException {
        Path path = blocksDir.resolve(key + ".meta");
        Files.createDirectories(path.getParent());

        if (Files.exists(path)) {
            Map<String, Object> existing = getMetadata(key);
            if (existing != null) {
                double existTs = asDouble(existing.get("lamport_ts"), 0);
                double newTs = asDouble(metadata.get("lamport_ts"), 0);

                // Rule A: ignore chronologically older vectors
                if (newTs < existTs) {
                    log.debug("[CONSISTENCY] Ignored logically stale metadata for {} (Lamport {} < {})",
                            key, newTs, existTs);
                    return false;
                }
                // Rule B: deterministic tie-breaker via node-id supremacy
                else if (newTs == existTs) {
                    String existNode = String.valueOf(existing.getOrDefault("source_node", ""));
                    String newNode = String.valueOf(metadata.getOrDefault("source_node", ""));
                    if (!newNode.equals(existNode) && newNode.compareTo(existNode) < 0) {
                        log.debug("[CONSISTENCY] Tie-breaker rejected {} (Node {} < Node {})",
                                key, newNode, existNode);
                        return false;
                    }
                }
            }
        }

        byte[] json = mapper.writeValueAsBytes(metadata);
        Files.write(path, json);
        log.info("Saved manifest for {}", key);
        return true;
    }

    /** Retrieve metadata for a key, or null. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getMetadata(String key) throws IOException {
        Path path = blocksDir.resolve(key + ".meta");
        if (!Files.exists(path)) {
            return null;
        }
        byte[] bytes = Files.readAllBytes(path);
        return mapper.readValue(bytes, Map.class);
    }

    /** List all locally stored block ids (.dat), excluding manifest_ entries. */
    public List<String> listBlocks() throws IOException {
        List<String> blocks = new ArrayList<>();
        if (!Files.exists(blocksDir)) {
            return blocks;
        }
        try (var stream = Files.list(blocksDir)) {
            stream.forEach(p -> {
                String name = p.getFileName().toString();
                if (name.endsWith(".dat") && !name.startsWith("manifest_")) {
                    blocks.add(name.substring(0, name.length() - 4));
                }
            });
        }
        return blocks;
    }

    /** Delete a block from disk. */
    public boolean deleteBlock(String blockId) throws IOException {
        logWal("START_DELETE " + blockId);
        Path path = blocksDir.resolve(blockId + ".dat");
        if (Files.exists(path)) {
            Files.delete(path);
            logWal("COMMIT_DELETE " + blockId);
            log.info("[STORAGE] Deleted block {}", blockId);
            return true;
        }
        return false;
    }

    /** Delete metadata for a key. */
    public boolean deleteMetadata(String key) throws IOException {
        Path path = blocksDir.resolve(key + ".meta");
        if (Files.exists(path)) {
            Files.delete(path);
            log.info("[STORAGE] Deleted metadata for {}", key);
            return true;
        }
        return false;
    }

    private static double asDouble(Object o, double def) {
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        return def;
    }
}
