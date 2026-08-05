package com.dfs.controller;

import com.dfs.config.ClusterConfig;
import com.dfs.config.RpcClient;
import com.dfs.consensus.ConsensusService;
import com.dfs.faulttolerance.NodeRegistry;
import com.dfs.model.LogEntry;
import com.dfs.replication.ReplicationManager;
import com.dfs.replication.StorageManager;
import com.dfs.timesync.LamportClock;
import com.dfs.util.HashUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * File CRUD endpoints.
 *
 * Writes (upload/delete) are leader-only and go through Raft, so every node
 * agrees on file metadata. Reads are served ONLY by the leader: a non-leader
 * forwards the read to the current leader, which guarantees a client always
 * observes committed state (read-your-writes / linearizable reads).
 *
 * Note on the strength of the guarantee: this routes reads to whoever the node
 * currently believes is leader. A fully rigorous implementation would add a
 * read-index (the leader confirms it still holds leadership via one heartbeat
 * round before serving) to rule out a briefly-partitioned stale leader. That is
 * the natural next step; leader-routed reads already eliminate the common
 * stale-read case of serving a follower's not-yet-replicated manifest.
 */
@RestController
public class FileController {

    private static final Logger log = LoggerFactory.getLogger(FileController.class);
    private static final int BLOCK_SIZE = 1024 * 1024; // 1MB blocks
    private static final Duration FORWARD_TIMEOUT = Duration.ofSeconds(10);

    private final ClusterConfig config;
    private final ConsensusService consensus;
    private final StorageManager storage;
    private final NodeRegistry registry;
    private final ReplicationManager replication;
    private final LamportClock lamportClock;
    private final RpcClient rpc;
    private final Timer uploadTimer;

    public FileController(ClusterConfig config, ConsensusService consensus, StorageManager storage,
            NodeRegistry registry, ReplicationManager replication, LamportClock lamportClock,
            RpcClient rpc, MeterRegistry meterRegistry) {
        this.config = config;
        this.consensus = consensus;
        this.storage = storage;
        this.registry = registry;
        this.replication = replication;
        this.lamportClock = lamportClock;
        this.rpc = rpc;
        this.uploadTimer = Timer.builder("dfs.file.upload.latency")
                .description("End-to-end file upload (write) latency on the leader")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    @PostMapping("/files/{filename}")
    public ResponseEntity<Object> uploadFile(@PathVariable String filename,
            @RequestParam("file") MultipartFile file) throws Exception {
        // 1. Consensus check - only the leader can coordinate writes.
        String leader = consensus.getCurrentLeader();
        if (!config.getNodeId().equals(leader)) {
            if (leader != null && config.getAllNodes().containsKey(leader)) {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("status", "error");
                body.put("message", "not leader");
                body.put("leader", leader);
                body.put("leader_url", config.urlFor(leader));
                return ResponseEntity.ok(body);
            }
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "No leader elected yet");
        }

        byte[] content = file.getBytes();
        if (content == null || content.length == 0) {
            log.error("Empty file received for {}", filename);
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Empty file uploaded");
        }

        Timer.Sample sample = Timer.start();

        log.info("Received {} bytes for {}. Splitting into blocks...", content.length, filename);
        List<Map<String, Object>> blocks = new ArrayList<>();

        List<String> liveNodes = registry.getLiveNodes();
        List<String> targetNodes = new ArrayList<>();
        for (String n : liveNodes) {
            if (!n.equals(config.getNodeId())) {
                targetNodes.add(n);
            }
        }

        for (int i = 0; i < content.length; i += BLOCK_SIZE) {
            int end = Math.min(i + BLOCK_SIZE, content.length);
            byte[] blockData = new byte[end - i];
            System.arraycopy(content, i, blockData, 0, end - i);
            String blockId = HashUtil.generateBlockId(filename, i / BLOCK_SIZE);

            storage.writeBlock(blockId, blockData);

            Map<String, Object> blockInfo = new LinkedHashMap<>();
            blockInfo.put("block_id", blockId);
            blockInfo.put("offset", i);
            blockInfo.put("size", blockData.length);
            blocks.add(blockInfo);

            if (!targetNodes.isEmpty()) {
                long lamportTs = lamportClock.tick();
                replication.replicateBlock(blockId, blockData, targetNodes, lamportTs);
            }
        }

        long lamportTsMeta = lamportClock.tick();

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("filename", filename);
        manifest.put("total_size", content.length);
        manifest.put("blocks", blocks);
        manifest.put("created", HashUtil.now());
        manifest.put("replicated_to", targetNodes);
        manifest.put("lamport_ts", lamportTsMeta);
        manifest.put("source_node", config.getNodeId());

        // Mirror the block manifest to followers so they can serve the blocks.
        if (!targetNodes.isEmpty()) {
            replication.replicateMetadata(filename, manifest, targetNodes, lamportTsMeta);
        }

        // Raft is used ONLY for metadata consensus (file manifests, leadership).
        // The commit callback persists the manifest on every node once committed.
        LogEntry entry = new LogEntry(0, 0, "CREATE_FILE", filename, manifest);
        boolean success = consensus.replicateLog(entry);
        if (!success) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Consensus failed for file metadata");
        }

        storage.saveMetadata("manifest_" + filename, manifest);
        sample.stop(uploadTimer);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "success");
        body.put("filename", filename);
        body.put("total_size", content.length);
        body.put("blocks", blocks.size());
        body.put("consensus", "committed");
        return ResponseEntity.ok(body);
    }

    @GetMapping("/files/{filename}")
    @SuppressWarnings("unchecked")
    public ResponseEntity<?> downloadFile(@PathVariable String filename) throws Exception {
        String leader = consensus.getCurrentLeader();
        if (leader == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "No leader elected yet");
        }

        // Linearizable reads: only the leader serves reads. A non-leader forwards
        // to the leader so the client always sees committed state.
        if (!config.getNodeId().equals(leader)) {
            String leaderUrl = config.urlFor(leader);
            if (leaderUrl == null) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Leader address unknown");
            }
            Optional<RpcClient.ProxyResponse> proxied =
                    rpc.proxyGet(leaderUrl + "/files/" + filename, FORWARD_TIMEOUT);
            if (proxied.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Leader unavailable");
            }
            RpcClient.ProxyResponse pr = proxied.get();
            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.CONTENT_TYPE, pr.contentType());
            if (pr.contentType() != null && pr.contentType().contains("octet-stream")) {
                headers.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
            }
            return new ResponseEntity<>(pr.body(), headers, HttpStatus.valueOf(pr.status()));
        }

        // We are the leader: serve from committed local state.
        Map<String, Object> manifest = storage.getMetadata("manifest_" + filename);
        if (manifest == null || manifest.isEmpty()) {
            return ResponseEntity.ok(Map.of("error", "File not found"));
        }

        List<Map<String, Object>> blocks = (List<Map<String, Object>>) manifest.get("blocks");
        java.io.ByteArrayOutputStream allData = new java.io.ByteArrayOutputStream();
        List<String> missing = new ArrayList<>();

        if (blocks != null) {
            for (Map<String, Object> blockInfo : blocks) {
                String blockId = String.valueOf(blockInfo.get("block_id"));
                byte[] data = storage.readBlock(blockId);
                if (data != null) {
                    allData.write(data);
                } else {
                    missing.add(blockId);
                }
            }
        }

        if (!missing.isEmpty()) {
            return ResponseEntity.ok(Map.of("error", "File corrupted, missing " + missing.size() + " blocks"));
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        headers.set(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
        return new ResponseEntity<>(allData.toByteArray(), headers, HttpStatus.OK);
    }

    @DeleteMapping("/files/{filename}")
    public ResponseEntity<Object> deleteFile(@PathVariable String filename) throws Exception {
        String leader = consensus.getCurrentLeader();
        if (!config.getNodeId().equals(leader)) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Not leader. Please contact " + leader);
        }

        Map<String, Object> manifest = storage.getMetadata("manifest_" + filename);
        if (manifest == null || manifest.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "File not found");
        }

        // Commit the delete through Raft. The commit callback removes the blocks
        // and manifest on EVERY node, so deletes are consistent cluster-wide
        // (a deleted file cannot reappear from a node that missed the delete).
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("filename", filename);
        LogEntry entry = new LogEntry(0, 0, "DELETE_FILE", filename, payload);
        boolean success = consensus.replicateLog(entry);
        if (!success) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Consensus failed for delete");
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "success");
        body.put("message", "Deleted " + filename);
        body.put("consensus", "committed");
        return ResponseEntity.ok(body);
    }
}
