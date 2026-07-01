package com.dfs.faulttolerance;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.dfs.config.ClusterConfig;
import com.dfs.model.NodeStatus;

/**
 * Keeps track of all live nodes in the cluster. Port of
 * features/fault_tolerance/registry.py.
 *
 * A node is "live" if it has sent a heartbeat within failure_timeout (6s).
 * get_live_nodes() prunes stale entries on each call, exactly like the original.
 *
 * Python guarded this with an asyncio.Lock; here a ReentrantLock provides the
 * same mutual exclusion across the real threads used by the heartbeat handler
 * and the fault-tolerance monitor loop.
 */
@Component
public class NodeRegistry {

    private static final Logger log = LoggerFactory.getLogger(NodeRegistry.class);

    /** A node is considered failed after this many seconds without a heartbeat. */
    public static final double FAILURE_TIMEOUT = 6.0;

    private final ClusterConfig config;
    private final String nodeId;
    private final int totalNodes;

    /** nodeId -> last-seen epoch seconds. */
    private final Map<String, Double> liveNodes = new LinkedHashMap<>();
    private final Map<String, NodeStatus> nodeStatus = new LinkedHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public NodeRegistry(ClusterConfig config) {
        this.config = config;
        this.nodeId = config.getNodeId();
        this.totalNodes = config.getAllNodes().size();
        for (String node : config.getAllNodes().keySet()) {
            nodeStatus.put(node, NodeStatus.HEALTHY);
        }
    }


}
