package com.dfs.controller;

import com.dfs.config.ClusterConfig;
import com.dfs.replication.StorageManager;
import com.dfs.timesync.LamportClock;
import com.dfs.util.HashUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

/**
 * Data-replication receive endpoints. Port of the /replicate and
 * /replicate_meta routes in main.py.
 *
 * These form the asynchronous replication layer that is independent of Raft:
 * a leader pushes blocks (hex-encoded in JSON for binary safety) and manifests
 * to its peers. Incoming Lamport timestamps advance the local causal clock.
 */
@RestController
public class ReplicationController {

    private static final Logger log = LoggerFactory.getLogger(ReplicationController.class);

    private final ClusterConfig config;
    private final StorageManager storage;
    private final LamportClock lamportClock;
    private final ObjectMapper mapper;

    public ReplicationController(ClusterConfig config, StorageManager storage,
            LamportClock lamportClock, ObjectMapper mapper) {
        this.config = config;
        this.storage = storage;
        this.lamportClock = lamportClock;
        this.mapper = mapper;
    }

    @PostMapping("/replicate")
    public Map<String, Object> receiveReplication(@RequestBody JsonNode data) throws Exception {
        String blockId = data.path("block_id").asText(null);
        String blockHex = data.path("data").asText(null);
        String sourceNode = data.path("source_node").asText("unknown");
        long lamportTs = data.path("lamport_ts").asLong(1);

        lamportClock.update(lamportTs);

        if (blockId == null || blockHex == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing block data");
        }

        byte[] blockData = HashUtil.fromHex(blockHex);
        storage.writeBlock(blockId, blockData);
        log.info("[NODE {}] REPLICATED: Block {} from {}", config.getNodeId(), blockId, sourceNode);
        return Map.of("status", "ok");
    }

    @PostMapping("/replicate_meta")
    @SuppressWarnings("unchecked")
    public Map<String, Object> replicateMeta(@RequestBody JsonNode data) throws Exception {
        String filename = data.path("filename").asText(null);
        long lamportTs = data.path("lamport_ts").asLong(1);

        lamportClock.update(lamportTs);

        Map<String, Object> manifest = mapper.convertValue(data.get("manifest"), Map.class);
        storage.saveMetadata("manifest_" + filename, manifest);
        log.info("[NODE {}] MIRROR: Manifest replicated for {}", config.getNodeId(), filename);
        return Map.of("status", "ok");
    }
}
