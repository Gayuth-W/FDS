package com.dfs.replication;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.dfs.config.ClusterConfig;
import com.dfs.config.RpcClient;
import com.dfs.util.HashUtil;

/**
 * Handles asynchronous replication of blocks and metadata to other nodes.
 * Port of features/replication/manager.py.
 *
 * Independent of Raft: blocks are pushed to peers' /replicate endpoint and
 * manifests to /replicate_meta. Failed targets are retried up to 3 attempts
 * with a 2s back-off, exactly like the original asyncio.Queue worker. Here the
 * queue is a BlockingQueue drained by a single virtual-thread worker.
 */
@Component
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private static final Duration RPC_TIMEOUT = Duration.ofSeconds(2);

    private final ClusterConfig config;
    private final RpcClient rpc;
    private final String nodeId;

    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();

    private final AtomicLong totalReplications = new AtomicLong();
    private final AtomicLong successful = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();

    private volatile boolean running = true;
    private Thread worker;

    public ReplicationManager(ClusterConfig config, RpcClient rpc) {
        this.config = config;
        this.rpc = rpc;
        this.nodeId = config.getNodeId();
        log.info("[REPLICATION] ReplicationManager initialized for {}", nodeId);
    }

    public void start() {
        running = true;
        worker = Thread.ofVirtual().name("replication-" + nodeId).start(this::processQueue);
        log.info("[REPLICATION] Replication processor started");
    }

    public void stop() {
        running = false;
        if (worker != null) {
            worker.interrupt();
        }
    }

    /** Queue a binary block for replication to the given targets. */
    public void replicateBlock(String blockId, byte[] data, List<String> targetNodes, long lamportTs) {
        Task t = new Task();
        t.type = "BLOCK";
        t.blockId = blockId;
        t.data = data;
        t.targets = targetNodes;
        t.attempts = 0;
        t.lamportTs = lamportTs;
        queue.offer(t);
        log.info("[REPLICATION] Queued block replication for {} to {} nodes", blockId, targetNodes.size());
    }

    /** Queue a metadata manifest for replication to the given targets. */
    public void replicateMetadata(String filename, Map<String, Object> manifest, List<String> targetNodes, long lamportTs) {
        Task t = new Task();
        t.type = "METADATA";
        t.filename = filename;
        t.manifest = manifest;
        t.targets = targetNodes;
        t.attempts = 0;
        t.lamportTs = lamportTs;
        queue.offer(t);
        log.info("[REPLICATION] Queued metadata replication for {} to {} nodes", filename, targetNodes.size());
    }

    private void processQueue() {
        while (running) {
            Task task;
            try {
                task = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                task.attempts += 1;

                Map<String, Boolean> results = "METADATA".equals(task.type)
                        ? replicateMetaToNodes(task)
                        : replicateToNodes(task);

                int ok = 0;
                int bad = 0;
                for (boolean v : results.values()) {
                    if (v) ok++; else bad++;
                }

                totalReplications.incrementAndGet();
                successful.addAndGet(ok);
                failed.addAndGet(bad);

                if (bad > 0 && task.attempts < 3) {
                    Thread.sleep(2000);
                    queue.offer(task);
                    retries.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Error in replication processor: {}", e.toString());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private Map<String, Boolean> replicateToNodes(Task task) {
        Map<String, Boolean> results = new LinkedHashMap<>();
        for (String peerId : task.targets) {
            String peerUrl = config.urlFor(peerId);
            if (peerUrl == null) {
                continue;
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("block_id", task.blockId);
            body.put("data", HashUtil.toHex(task.data));
            body.put("source_node", nodeId);
            body.put("lamport_ts", task.lamportTs);
            boolean ok = rpc.postOk(peerUrl + "/replicate", body, RPC_TIMEOUT);
            results.put(peerId, ok);
            if (ok) {
                log.info("[REPLICATION] Replicated block {} to {}", task.blockId, peerId);
            }
        }
        return results;
    }

    private Map<String, Boolean> replicateMetaToNodes(Task task) {
        Map<String, Boolean> results = new LinkedHashMap<>();
        for (String peerId : task.targets) {
            String peerUrl = config.urlFor(peerId);
            if (peerUrl == null) {
                continue;
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("filename", task.filename);
            body.put("manifest", task.manifest);
            body.put("lamport_ts", task.lamportTs);
            boolean ok = rpc.postOk(peerUrl + "/replicate_meta", body, RPC_TIMEOUT);
            results.put(peerId, ok);
            if (ok) {
                log.info("[REPLICATION] Mirrored metadata {} to {}", task.filename, peerId);
            }
        }
        return results;
    }

    /** Internal replication task (mutable so attempts can be incremented on retry). */
    private static final class Task {
        String type;
        String blockId;
        byte[] data;
        String filename;
        Map<String, Object> manifest;
        List<String> targets;
        int attempts;
        long lamportTs;
    }
}
