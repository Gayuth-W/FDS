package com.dfs.controller;

import com.dfs.config.ClusterConfig;
import com.dfs.consensus.ConsensusService;
import com.dfs.faulttolerance.FaultToleranceManager;
import com.dfs.faulttolerance.NodeRegistry;
import com.dfs.faulttolerance.RecoveryManager;
import com.dfs.model.NodeStatus;
import com.dfs.replication.ReplicationManager;
import com.dfs.replication.StorageManager;
import com.dfs.timesync.LamportClock;
import com.dfs.util.HashUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Operational endpoints: health, heartbeat, time, status, metrics and recovery.
 * Port of the corresponding routes in main.py. The /status and /api/metrics
 * shapes match the original exactly so the Streamlit dashboard renders without
 * changes.
 */
@RestController
public class ClusterController {

    private final ClusterConfig config;
    private final ConsensusService consensus;
    private final StorageManager storage;
    private final NodeRegistry registry;
    private final ReplicationManager replication;
    private final RecoveryManager recovery;
    private final FaultToleranceManager faultTolerance;
    private final LamportClock lamportClock;
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;

    public ClusterController(ClusterConfig config, ConsensusService consensus, StorageManager storage,
            NodeRegistry registry, ReplicationManager replication, RecoveryManager recovery,
            FaultToleranceManager faultTolerance, LamportClock lamportClock,
            com.fasterxml.jackson.databind.ObjectMapper mapper) {
        this.config = config;
        this.consensus = consensus;
        this.storage = storage;
        this.registry = registry;
        this.replication = replication;
        this.recovery = recovery;
        this.faultTolerance = faultTolerance;
        this.lamportClock = lamportClock;
        this.mapper = mapper;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("node", config.getNodeId());
        body.put("state", consensus.raft().getState().wire());
        return body;
    }

    @PostMapping("/heartbeat")
    public Map<String, Object> receiveHeartbeat(@RequestBody JsonNode data) {
        String nodeId = data.path("node_id").asText(null);
        if (nodeId != null) {
            registry.registerNode(nodeId);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("time", HashUtil.now());
        return body;
    }

    @GetMapping("/time")
    public Map<String, Object> getTime() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("node_id", config.getNodeId());
        body.put("time", HashUtil.now());
        return body;
    }

    @GetMapping({ "/status", "/api/status" })
    public Map<String, Object> status() throws Exception {
        Map<String, Object> systemStatus = faultTolerance.getSystemStatus();
        Map<String, Object> replicationStats = replication.getStats();
        Map<String, Object> checkpointInfo = recovery.getCheckpointInfo();
        int blockCount = storage.listBlocks().size();

        Map<String, Object> raftState = new LinkedHashMap<>();
        raftState.put("state", consensus.raft().getState().wire());
        raftState.put("term", consensus.raft().getCurrentTerm());
        raftState.put("leader", consensus.raft().getLeaderId());
        raftState.put("commit_index", consensus.raft().getCommitIndex());

        Map<String, Object> storageInfo = new LinkedHashMap<>();
        storageInfo.put("block_count", blockCount);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("node_id", config.getNodeId());
        body.put("raft", raftState);
        body.put("system", systemStatus);
        body.put("storage", storageInfo);
        body.put("replication", replicationStats);
        body.put("checkpoint", checkpointInfo);
        return body;
    }

    @GetMapping("/api/metrics")
    public Map<String, Object> metrics() throws Exception {
        boolean isLeader = consensus.isLeader();

        Map<String, Object> raftState = new LinkedHashMap<>();
        raftState.put("state", consensus.raft().getState().wire());
        raftState.put("leader", consensus.raft().getLeaderId());
        raftState.put("term", consensus.raft().getCurrentTerm());
        raftState.put("vote_count", isLeader ? 1 : 0);

        int blockCount = storage.listBlocks().size();
        List<String> liveNodes = registry.getLiveNodes();
        List<String> peers = new ArrayList<>();
        for (String n : liveNodes) {
            if (!n.equals(config.getNodeId())) {
                peers.add(n);
            }
        }
        int peerCount = liveNodes.contains(config.getNodeId()) ? liveNodes.size() - 1 : liveNodes.size();

        Map<String, Object> replicationState = new LinkedHashMap<>();
        replicationState.put("files_stored", blockCount);
        replicationState.put("peer_count", peerCount);
        replicationState.put("peers", peers);

        Map<String, NodeStatus> nodeStatus = faultTolerance.getNodeStatusMap();
        int suspected = 0;
        int failed = 0;
        Map<String, Object> statusDetails = new LinkedHashMap<>();
        for (Map.Entry<String, NodeStatus> e : nodeStatus.entrySet()) {
            if (e.getValue() == NodeStatus.SUSPECTED)
                suspected++;
            if (e.getValue() == NodeStatus.FAILED)
                failed++;
            statusDetails.put(e.getKey(), e.getValue().value());
        }

        Map<String, Object> faultState = new LinkedHashMap<>();
        faultState.put("healthy_counts", liveNodes.size());
        faultState.put("suspected_counts", suspected);
        faultState.put("failed_counts", failed);
        faultState.put("node_status_details", statusDetails);

        Map<String, Object> timeState = new LinkedHashMap<>();
        timeState.put("protocol", "OS Native NTP + Lamport Causality");
        timeState.put("last_sync_timestamp", HashUtil.now());
        timeState.put("lamport_counter", lamportClock.getTime());

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("consensus", raftState);
        body.put("replication", replicationState);
        body.put("fault_tolerance", faultState);
        body.put("time_sync", timeState);
        return body;
    }

    @PostMapping("/recovery/plan")
    public ResponseEntity<Object> planRecovery(@RequestBody JsonNode data) {
        String failedNode = data.path("failed_node").asText(null);
        if (failedNode == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "Missing failed_node"));
        }
        Map<String, Object> fileMap = faultTolerance.getFileMap();
        Map<String, List<String>> plan = recovery.planRecovery(failedNode, fileMap);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("failed_node", failedNode);
        body.put("recovery_plan", plan);
        return ResponseEntity.ok(body);
    }

    @PostMapping("/recovery/execute")
    @SuppressWarnings("unchecked")
    public ResponseEntity<Object> executeRecovery(@RequestBody JsonNode data) {
        String recoveringNode = data.path("recovering_node").asText(null);
        if (recoveringNode == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "Missing recovering_node"));
        }
        Map<String, List<String>> plan = Map.of();
        JsonNode planNode = data.get("recovery_plan");
        if (planNode != null && !planNode.isNull()) {
            plan = mapper.convertValue(planNode, Map.class);
        }
        recovery.executeRecovery(recoveringNode, plan);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "complete");
        body.put("recovering_node", recoveringNode);
        return ResponseEntity.ok(body);
    }
}
