package com.dfs.timesync;

import com.dfs.config.ClusterConfig;
import com.dfs.config.RpcClient;
import com.dfs.consensus.ConsensusService;
import com.dfs.model.ClockStatus;
import com.dfs.model.FallbackStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Monitors time synchronisation across the cluster. Port of
 * features/time_sync/monitor.py.
 *
 * Like the original, this build relies on OS-level (NTP) time plus Lamport
 * causality, so the health check bypasses external offset math and simply
 * reports the current fallback strategy. The reference clock is the current
 * Raft leader.
 */
@Component
public class TimeSyncMonitor {

    private final ClusterConfig config;
    private final RpcClient rpc;
    private final ConsensusService consensus;

    private final int maxOffsetMs;
    private volatile List<ClockStatus> lastStatus;
    private volatile FallbackStrategy currentFallback = FallbackStrategy.WARN_ONLY;
    private volatile Double lastCheckTime;

    public TimeSyncMonitor(ClusterConfig config, RpcClient rpc, ConsensusService consensus) {
        this.config = config;
        this.rpc = rpc;
        this.consensus = consensus;
        this.maxOffsetMs = 50;
    }

    /** Collect current times from all live nodes via GET /time. */
    public Map<String, Double> getNodeTimes() {
        Map<String, Double> nodeTimes = new LinkedHashMap<>();
        nodeTimes.put(config.getNodeId(), now());

        for (Map.Entry<String, String> e : config.getOtherNodes().entrySet()) {
            Optional<JsonNode> resp = rpc.getJson(e.getValue() + "/time", Duration.ofMillis(500));
            resp.ifPresent(node -> {
                JsonNode t = node.get("time");
                if (t != null && t.isNumber()) {
                    nodeTimes.put(e.getKey(), t.asDouble());
                }
            });
        }
        return nodeTimes;
    }

    /** Current reference node = Raft leader (falls back to self). */
    public String getReferenceNode() {
        String leader = consensus.getCurrentLeader();
        return leader != null ? leader : config.getNodeId();
    }

    /**
     * Check cluster health and update internal state. OS time-sync logic
     * bypasses external offset checks, exactly as in the original.
     */
    public synchronized List<ClockStatus> checkClusterHealth() {
        List<ClockStatus> statuses = List.of();
        this.lastStatus = statuses;
        this.lastCheckTime = now();
        return statuses;
    }

    public List<ClockStatus> getCurrentStatus() {
        return lastStatus;
    }

    public FallbackStrategy getFallbackStrategy() {
        return currentFallback;
    }

    public Double getLastCheckTime() {
        return lastCheckTime;
    }

    private static double now() {
        return System.currentTimeMillis() / 1000.0;
    }
}
