package com.dfs.config;

import com.dfs.consensus.ConsensusService;
import com.dfs.faulttolerance.FaultToleranceManager;
import com.dfs.faulttolerance.NodeRegistry;
import com.dfs.faulttolerance.RecoveryManager;
import com.dfs.replication.ReplicationManager;
import com.dfs.replication.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Files;

/**
 * Startup wiring for a node. Port of the FastAPI lifespan startup block in
 * main.py.
 *
 * Ensures local data directories exist, registers the Raft commit callback that
 * mirrors a committed CREATE_FILE manifest into local metadata storage, starts
 * the replication / recovery / fault-tolerance background workers, and registers
 * this node as active. (The Raft election and log-replication managers are
 * started separately by ConsensusService.)
 */
@Component
public class ClusterBootstrap implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ClusterBootstrap.class);

    private final ClusterConfig config;
    private final StorageManager storage;
    private final ConsensusService consensus;
    private final ReplicationManager replication;
    private final RecoveryManager recovery;
    private final FaultToleranceManager faultTolerance;
    private final NodeRegistry registry;

    public ClusterBootstrap(ClusterConfig config, StorageManager storage, ConsensusService consensus,
                            ReplicationManager replication, RecoveryManager recovery,
                            FaultToleranceManager faultTolerance, NodeRegistry registry) {
        this.config = config;
        this.storage = storage;
        this.consensus = consensus;
        this.replication = replication;
        this.recovery = recovery;
        this.faultTolerance = faultTolerance;
        this.registry = registry;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Starting node {} on port {}", config.getNodeId(), config.getPort());

        Files.createDirectories(config.getBlocksDir());
        Files.createDirectories(config.getCheckpointDir());

        // Event-driven metadata replication: when a CREATE_FILE entry commits,
        // persist its manifest locally (the "mirror sync" in the original).
        consensus.registerCommitCallback(entry -> {
            if ("CREATE_FILE".equals(entry.getOp())) {
                String filename = entry.getFileId();
                storage.saveMetadata("manifest_" + filename, entry.getPayload());
                log.info("[NODE {}] MIRROR SYNC: Saved metadata for {}", config.getNodeId(), filename);
            }
        });

        replication.start();
        recovery.start();
        faultTolerance.start();

        // Register self as active.
        registry.registerNode(config.getNodeId());
        log.info("Node {} fully started with Raft and Time Sync", config.getNodeId());
    }
}
