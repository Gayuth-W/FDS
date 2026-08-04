package com.dfs.controller;

import com.dfs.config.ClusterConfig;
import com.dfs.consensus.ConsensusService;
import com.dfs.faulttolerance.NodeRegistry;
import com.dfs.model.LogEntry;
import com.dfs.replication.ReplicationManager;
import com.dfs.replication.StorageManager;
import com.dfs.timesync.LamportClock;
import com.dfs.util.HashUtil;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * File CRUD endpoints. Port of the /files routes in main.py.
 *
 * Upload is leader-only: the file is split into 1MB blocks, blocks are
 * replicated asynchronously, and the manifest is committed through Raft so
 * every node agrees on file metadata. The JSON shapes are identical to the
 * original so the existing Streamlit dashboard and CLI client work unchanged.
 */
@RestController
public class FileController {

    private static final Logger log = LoggerFactory.getLogger(FileController.class);
    private static final int BLOCK_SIZE = 1024 * 1024; // 1MB blocks

    private final ClusterConfig config;
    private final ConsensusService consensus;
    private final StorageManager storage;
    private final NodeRegistry registry;
    private final ReplicationManager replication;
    private final LamportClock lamportClock;

    public FileController(ClusterConfig config, ConsensusService consensus, StorageManager storage,
            NodeRegistry registry, ReplicationManager replication, LamportClock lamportClock) {
        this.config = config;
        this.consensus = consensus;
        this.storage = storage;
        this.registry = registry;
        this.replication = replication;
        this.lamportClock = lamportClock;
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

        // Mirror the manifest to followers immediately (Option 1 replication).
        if (!targetNodes.isEmpty()) {
            replication.replicateMetadata(filename, manifest, targetNodes, lamportTsMeta);
        }

        // Raft is used ONLY for metadata consensus (file manifests, leadership).
        LogEntry entry = new LogEntry(0, 0, "CREATE_FILE", filename, manifest);
        boolean success = consensus.replicateLog(entry);
        if (!success) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Consensus failed for file metadata");
        }

        storage.saveMetadata("manifest_" + filename, manifest);

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
    @SuppressWarnings("unchecked")
    public ResponseEntity<Object> deleteFile(@PathVariable String filename) throws Exception {
        String leader = consensus.getCurrentLeader();
        if (!config.getNodeId().equals(leader)) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Not leader. Please contact " + leader);
        }

        Map<String, Object> manifest = storage.getMetadata("manifest_" + filename);
        if (manifest == null || manifest.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "File not found");
        }

        List<Map<String, Object>> blocks = (List<Map<String, Object>>) manifest.get("blocks");
        if (blocks != null) {
            for (Map<String, Object> blockInfo : blocks) {
                storage.deleteBlock(String.valueOf(blockInfo.get("block_id")));
            }
        }
        storage.deleteMetadata("manifest_" + filename);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "success");
        body.put("message", "Deleted " + filename);
        return ResponseEntity.ok(body);
    }
}
