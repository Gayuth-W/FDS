package com.dfs.faulttolerance;

import com.dfs.config.ClusterConfig;
import com.dfs.model.NodeStatus;
import com.dfs.replication.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

}
