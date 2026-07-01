package com.dfs.faulttolerance;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.dfs.config.ClusterConfig;
import com.dfs.model.NodeStatus;
import com.dfs.replication.StorageManager;

/**
 * Main fault-tolerance coordinator. Port of features/fault_tolerance/manager.py.
 *
 * Runs a 1s monitor loop that classifies every peer as HEALTHY / SUSPECTED /
 * FAILED purely from heartbeat recency:
 *   - seen within suspect_timeout (2s)  -> HEALTHY (and triggers recovery if it
 *     was previously FAILED)
 *   - 2s..4s since last seen            -> SUSPECTED
 *   - over failure_timeout (4s)         -> FAILED (and schedules recovery planning)
 *
 * Recovery is run on a separate virtual thread so the monitor loop never blocks,
 * mirroring the asyncio.create_task calls in the original.
 *
 * Note: the original constructor also received the replication manager but never
 * used it in this class; data replication is driven by the async pipeline in
 * ReplicationManager, so it is intentionally not a dependency here.
 */
@Component
public class FaultToleranceManager {

    private static final Logger log = LoggerFactory.getLogger(FaultToleranceManager.class);

    public static final double SUSPECT_TIMEOUT = 2.0;   // ~1 missed heartbeat
    public static final double FAILURE_TIMEOUT = 4.0;   // ~3 missed heartbeats
    public static final long HEARTBEAT_INTERVAL_SEC = 1; // monitor loop wait

    private final ClusterConfig config;
    private final StorageManager storage;
    private final NodeRegistry registry;
    private final RecoveryManager recovery;

    private final String nodeId;
    private final Map<String, NodeStatus> nodeStatus = new ConcurrentHashMap<>();

    private volatile boolean running = true;
    private final double startTime = now();
    private Thread monitorThread;

    public FaultToleranceManager(ClusterConfig config, StorageManager storage,
                                 NodeRegistry registry, RecoveryManager recovery) {
        this.config = config;
        this.storage = storage;
        this.registry = registry;
        this.recovery = recovery;
        this.nodeId = config.getNodeId();
        log.info("FaultToleranceManager initialized for {}", nodeId);
    }

    public void start() {
        running = true;
        monitorThread = Thread.ofVirtual().name("ft-monitor-" + nodeId).start(this::monitorFailures);
        log.info("Fault tolerance monitoring started");
    }

    public void stop() {
        running = false;
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
    }

    private void monitorFailures() {
        while (running) {
            try {
                Thread.sleep(Duration.ofSeconds(HEARTBEAT_INTERVAL_SEC));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            double now = now();
            Map<String, Double> lastSeen = registry.liveNodesSnapshot();

            for (String node : getAllNodes()) {
                if (node.equals(nodeId)) {
                    continue;
                }
                double lastSeenTime = lastSeen.getOrDefault(node, 0.0);
                double sinceSeen = now - lastSeenTime;
                NodeStatus current = nodeStatus.getOrDefault(node, NodeStatus.HEALTHY);

                if (sinceSeen <= SUSPECT_TIMEOUT) {
                    if (current != NodeStatus.HEALTHY) {
                        if (current == NodeStatus.FAILED) {
                            log.info("[FAULT] Node {} is BACK ONLINE and RECOVERING", node);
                            nodeStatus.put(node, NodeStatus.RECOVERING);
                            String recovering = node;
                            Thread.ofVirtual().name("ft-recover-" + recovering).start(() -> runAsyncRecovery(recovering));
                        } else if (current == NodeStatus.SUSPECTED) {
                            nodeStatus.put(node, NodeStatus.HEALTHY);
                            log.info("[FAULT] Node {} recovered from SUSPECTED to HEALTHY", node);
                        }
                    }
                } else if (sinceSeen <= FAILURE_TIMEOUT) {
                    if (current == NodeStatus.HEALTHY) {
                        nodeStatus.put(node, NodeStatus.SUSPECTED);
                        log.warn("[FAULT] Node {} is SUSPECTED (missed heartbeat)", node);
                    }
                } else {
                    if (current == NodeStatus.SUSPECTED || current == NodeStatus.HEALTHY) {
                        nodeStatus.put(node, NodeStatus.FAILED);
                        log.error("[FAULT] Node {} has FAILED", node);
                        Map<String, Object> fileMap = getFileMap();
                        String failed = node;
                        Thread.ofVirtual().name("ft-fail-" + failed)
                                .start(() -> recovery.handleNodeFailure(failed, fileMap));
                    }
                }
            }
        }
    }

    private void runAsyncRecovery(String node) {
        Map<String, Object> fileMap = getFileMap();
        recovery.handleNodeRecovery(node, fileMap);
        nodeStatus.put(node, NodeStatus.HEALTHY);
        log.info("[FAULT] Node {} fully synced and state is now HEALTHY", node);
    }

    private List<String> getAllNodes() {
        return new ArrayList<>(config.getAllNodes().keySet());
    }

    /**
     * Mapping of files to which nodes have them. Simplified exactly as in the
     * original: exposes the cluster membership and a local block count under
     * the reserved "_"-prefixed keys.
     */
    public Map<String, Object> getFileMap() {
        Map<String, Object> fileMap = new LinkedHashMap<>();
        fileMap.put("_all_nodes", getAllNodes());
        int blockCount = 0;
        try {
            blockCount = storage.listBlocks().size();
        } catch (Exception e) {
            log.debug("getFileMap listBlocks failed: {}", e.toString());
        }
        fileMap.put("_total_blocks", blockCount);
        return fileMap;
    }

    public Map<String, Object> getSystemStatus() {
        List<String> liveNodes = registry.getLiveNodes();
        List<String> allNodes = getAllNodes();
        if (!liveNodes.contains(nodeId)) {
            liveNodes.add(nodeId);
        }

        String status = recovery.systemStatus(liveNodes);

        List<String> failedNodes = new ArrayList<>();
        for (String n : allNodes) {
            if (!liveNodes.contains(n) && !n.equals(nodeId)) {
                failedNodes.add(n);
            }
        }
        List<String> suspectedNodes = new ArrayList<>();
        for (Map.Entry<String, NodeStatus> e : nodeStatus.entrySet()) {
            if (e.getValue() == NodeStatus.SUSPECTED) {
                suspectedNodes.add(e.getKey());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("node_id", nodeId);
        result.put("status", status);
        result.put("live_nodes", liveNodes);
        result.put("failed_nodes", failedNodes);
        result.put("suspected_nodes", suspectedNodes);
        result.put("recovering", recovery.isRecovering());
        result.put("checkpoints", recovery.getCheckpointInfo());
        return result;
    }

    public boolean isMajority() {
        List<String> liveNodes = registry.getLiveNodes();
        if (!liveNodes.contains(nodeId)) {
            liveNodes.add(nodeId);
        }
        return liveNodes.size() > getAllNodes().size() / 2;
    }

    /** Exposed for the /api/metrics endpoint. */
    public Map<String, NodeStatus> getNodeStatusMap() {
        return nodeStatus;
    }

    public double getStartTime() {
        return startTime;
    }

    private static double now() {
        return System.currentTimeMillis() / 1000.0;
    }
}
