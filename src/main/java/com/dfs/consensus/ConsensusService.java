package com.dfs.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Facade over the Raft node and its managers. Port of the module-level public
 * API in features/consensus/consensus_impl.py.
 *
 * Owns startup/shutdown ordering of the election and replication managers
 * (equivalent to init_raft_node / stop_raft).
 */
@Service
public class ConsensusService {

    private static final Logger log = LoggerFactory.getLogger(ConsensusService.class);

    private final RaftNode raft;
    private final ElectionManager electionManager;
    private final LogReplicationManager replicationManager;

    public ConsensusService(RaftNode raft,
                            ElectionManager electionManager,
                            LogReplicationManager replicationManager) {
        this.raft = raft;
        this.electionManager = electionManager;
        this.replicationManager = replicationManager;
    }

    @PostConstruct
    void startRaft() {
        electionManager.start();
        replicationManager.start();
        log.info("Raft node {} initialized with peers: {}", raft.getNodeId(), raft.getPeerIds());
    }

    @PreDestroy
    void stopRaft() {
        log.info("Stopping Raft...");
        electionManager.stop();
        log.info("Election manager stopped");
        replicationManager.stop();
        log.info("Replication manager stopped");
        raft.stop();
        log.info("Raft shutdown complete");
    }

}
