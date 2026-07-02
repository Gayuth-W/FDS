package com.dfs.faulttolerance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.dfs.config.ClusterConfig;
import com.dfs.config.RpcClient;
import com.dfs.replication.StorageManager;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Manages recovery for failed nodes and periodic checkpointing. Port of
 * features/fault_tolerance/recovery.py.
 *
 * Difference from the original: the Python version serialises checkpoints with
 * pickle; here checkpoints are stored as JSON, which is portable and avoids the
 * Java-serialisation footguns. The on-disk layout (one timestamped file per
 * checkpoint, keep the newest 3) is preserved.
 */
@Component
public class RecoveryManager {

    private static final Logger log = LoggerFactory.getLogger(RecoveryManager.class);
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    /** Save a checkpoint every this many seconds. */
    public static final long CHECKPOINT_INTERVAL_SEC = 30;

    private final ClusterConfig config;
    private final StorageManager storage;
    private final RpcClient rpc;
    private final ObjectMapper mapper;

    private final String nodeId;
    private final Path checkpointDir;

    private volatile boolean running = true;
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private volatile int checkpointVersion = 0;
    private volatile double lastCheckpointTime = 0;

    private Thread checkpointThread;

    public RecoveryManager(ClusterConfig config, StorageManager storage, RpcClient rpc, ObjectMapper mapper) {
        this.config = config;
        this.storage = storage;
        this.rpc = rpc;
        this.mapper = mapper;
        this.nodeId = config.getNodeId();
        this.checkpointDir = config.getCheckpointDir();
        try {
            Files.createDirectories(checkpointDir);
        } catch (IOException e) {
            log.error("Failed to create checkpoint dir {}: {}", checkpointDir, e.toString());
        }
        log.info("RecoveryManager initialized for {}", nodeId);
    }

    /** Start periodic checkpointing on a virtual thread. */
    public void start() {
        running = true;
        checkpointThread = Thread.ofVirtual().name("checkpoint-" + nodeId).start(this::checkpointLoop);
        log.info("Checkpointing started (every {}s)", CHECKPOINT_INTERVAL_SEC);
    }

    public void stop() {
        running = false;
        if (checkpointThread != null) {
            checkpointThread.interrupt();
        }
    }

    private void checkpointLoop() {
        while (running) {
            try {
                Thread.sleep(Duration.ofSeconds(CHECKPOINT_INTERVAL_SEC));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            saveCheckpoint();
        }
    }

    public synchronized void saveCheckpoint() {
        try {
            checkpointVersion += 1;

            List<String> blocks = storage.listBlocks();
            Map<String, Object> metadata = new LinkedHashMap<>();
            for (String blockId : blocks) {
                Map<String, Object> meta = storage.getMetadata(blockId);
                if (meta != null && !meta.isEmpty()) {
                    metadata.put(blockId, meta);
                }
            }

            Map<String, Object> checkpoint = new LinkedHashMap<>();
            checkpoint.put("node_id", nodeId);
            checkpoint.put("timestamp", now());
            checkpoint.put("version", checkpointVersion);
            checkpoint.put("block_count", blocks.size());
            checkpoint.put("blocks", blocks);
            checkpoint.put("metadata", metadata);
            checkpoint.put("last_checkpoint", lastCheckpointTime);

            String filename = "checkpoint_" + LocalDateTime.now().format(STAMP) + "_" + checkpointVersion + ".json";
            Path path = checkpointDir.resolve(filename);
            mapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), checkpoint);

            lastCheckpointTime = now();
            log.info("Checkpoint saved: {} blocks (version {})", blocks.size(), checkpointVersion);

            cleanupOldCheckpoints(3);
        } catch (Exception e) {
            log.error("Failed to save checkpoint: {}", e.toString());
        }
    }

    private void cleanupOldCheckpoints(int keep) {
        try {
            List<Path> files = listCheckpointFiles();
            files.sort(Comparator.comparingLong((Path p) -> p.toFile().lastModified()).reversed());
            for (int i = 0; i < files.size(); i++) {
                if (i >= keep) {
                    Files.deleteIfExists(files.get(i));
                    log.info("Removed old checkpoint: {}", files.get(i).getFileName());
                }
            }
        } catch (Exception e) {
            log.error("Failed to cleanup checkpoints: {}", e.toString());
        }
    }

    public Map<String, Object> loadLatestCheckpoint() {
        try {
            List<Path> files = listCheckpointFiles();
            if (files.isEmpty()) {
                log.info("No checkpoints found");
                return null;
            }
            files.sort(Comparator.comparingLong((Path p) -> p.toFile().lastModified()).reversed());
            Path latest = files.get(0);
            @SuppressWarnings("unchecked")
            Map<String, Object> checkpoint = mapper.readValue(latest.toFile(), Map.class);
            log.info("Loaded checkpoint: {} ({} blocks)", latest.getFileName(), checkpoint.get("block_count"));
            return checkpoint;
        } catch (Exception e) {
            log.error("Failed to load checkpoint: {}", e.toString());
            return null;
        }
    }

    /**
     * Plan recovery for a failed node.
     * Returns {file_id: [source_nodes]} for what needs to be replicated.
     */
    @SuppressWarnings("unchecked")
    public Map<String, List<String>> planRecovery(String failedNodeId, Map<String, Object> fileMap) {
        log.info("Planning recovery for failed node: {}", failedNodeId);
        Map<String, List<String>> plan = new LinkedHashMap<>();

        for (Map.Entry<String, Object> e : fileMap.entrySet()) {
            String fileId = e.getKey();
            // Skip internal metadata keys like _total_blocks, _all_nodes.
            if (fileId.startsWith("_") || !(e.getValue() instanceof List)) {
                continue;
            }
            List<String> nodes = (List<String>) e.getValue();
            if (nodes.contains(failedNodeId)) {
                List<String> sources = new ArrayList<>();
                for (String n : nodes) {
                    if (!n.equals(failedNodeId)) {
                        sources.add(n);
                    }
                }
                if (!sources.isEmpty()) {
                    plan.put(fileId, sources);
                }
            }
        }

        log.info("Recovery plan: {} files need restoration", plan.size());
        return plan;
    }

    /**
     * Execute recovery by copying missing data to the recovering node: pull each
     * file from a source peer's /files endpoint and re-upload it to the
     * recovering node's /files endpoint.
     */
    public void executeRecovery(String recoveringNode, Map<String, List<String>> recoveryPlan) {
        recovering.set(true);
        log.info("Executing recovery for node {}", recoveringNode);
        try {
            int recovered = 0;
            int failed = 0;

            for (Map.Entry<String, List<String>> e : recoveryPlan.entrySet()) {
                String fileId = e.getKey();
                List<String> sources = e.getValue();
                if (sources.isEmpty()) {
                    continue;
                }
                String sourceUrl = config.urlFor(sources.get(0));
                if (sourceUrl == null) {
                    continue;
                }
                try {
                    Optional<byte[]> content = rpc.getBytes(sourceUrl + "/files/" + fileId, Duration.ofSeconds(5));
                    if (content.isPresent()) {
                        String recoverUrl = config.urlFor(recoveringNode);
                        if (recoverUrl != null) {
                            boolean ok = rpc.postMultipartFile(recoverUrl + "/files/" + fileId,
                                    fileId, content.get(), Duration.ofSeconds(5));
                            if (ok) {
                                recovered++;
                            } else {
                                failed++;
                            }
                        }
                    } else {
                        failed++;
                    }
                } catch (Exception ex) {
                    log.error("Failed to recover {}: {}", fileId, ex.toString());
                    failed++;
                }
            }

            log.info("Recovery complete: {} files restored, {} failed", recovered, failed);
        } catch (Exception e) {
            log.error("Error during recovery execution: {}", e.toString());
        } finally {
            recovering.set(false);
        }
    }

    /**
     * Determine system status based on live nodes.
     * Returns HEALTHY / DEGRADED / FAILED.
     */
    public String systemStatus(List<String> liveNodes) {
        int totalNodes = config.getAllNodes().size();
        int requiredQuorum = (totalNodes / 2) + 1;
        int liveCount = liveNodes.size();
        if (liveCount == totalNodes) {
            return "HEALTHY";
        } else if (liveCount >= requiredQuorum) {
            return "DEGRADED";
        }
        return "FAILED";
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> handleNodeFailure(String failedNode, Map<String, Object> fileMap) {
        log.warn("Node {} has FAILED", failedNode);

        List<String> allNodes = (List<String>) fileMap.getOrDefault("_all_nodes", new ArrayList<String>());
        List<String> liveNodes = new ArrayList<>();
        for (String n : allNodes) {
            if (!n.equals(failedNode)) {
                liveNodes.add(n);
            }
        }
        String status = systemStatus(liveNodes);
        if ("DEGRADED".equals(status)) {
            log.warn("System is DEGRADED (quorum maintained)");
        } else if ("FAILED".equals(status)) {
            log.error("System FAILED - cannot maintain quorum");
        }

        Map<String, List<String>> recoveryPlan = planRecovery(failedNode, fileMap);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("failed_node", failedNode);
        result.put("system_status", status);
        result.put("recovery_plan", recoveryPlan);
        result.put("files_to_recover", recoveryPlan.size());
        return result;
    }

    public Map<String, Object> handleNodeRecovery(String recoveredNode, Map<String, Object> fileMap) {
        log.info("Node {} is RECOVERING", recoveredNode);
        Map<String, List<String>> recoveryPlan = planRecovery(recoveredNode, fileMap);
        if (!recoveryPlan.isEmpty()) {
            executeRecovery(recoveredNode, recoveryPlan);
            log.info("Node {} recovery complete", recoveredNode);
        } else {
            log.info("Node {} already in sync", recoveredNode);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("recovered_node", recoveredNode);
        result.put("files_restored", recoveryPlan.size());
        return result;
    }

    public Map<String, Object> getCheckpointInfo() {
        List<Map<String, Object>> files = new ArrayList<>();
        for (Path p : listCheckpointFiles()) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("name", p.getFileName().toString());
            info.put("size", p.toFile().length());
            info.put("modified", p.toFile().lastModified() / 1000.0);
            files.add(info);
        }
        files.sort(Comparator.comparingDouble((Map<String, Object> m) -> ((Number) m.get("modified")).doubleValue())
                .reversed());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("checkpoint_dir", checkpointDir.toString());
        result.put("total_checkpoints", files.size());
        result.put("latest_checkpoint", files.isEmpty() ? null : files.get(0));
        result.put("checkpoints", files.subList(0, Math.min(5, files.size())));
        return result;
    }

    public boolean isRecovering() {
        return recovering.get();
    }

    private List<Path> listCheckpointFiles() {
        List<Path> files = new ArrayList<>();
        try {
            if (!Files.isDirectory(checkpointDir)) {
                return files;
            }
            Files.list(checkpointDir).forEach(p -> {
                String name = p.getFileName().toString();
                if (name.startsWith("checkpoint_") && name.endsWith(".json")) {
                    files.add(p);
                }
            });
        } catch (IOException e) {
            log.error("Failed to list checkpoints: {}", e.toString());
        }
        return files;
    }

    private static double now() {
        return System.currentTimeMillis() / 1000.0;
    }
}
