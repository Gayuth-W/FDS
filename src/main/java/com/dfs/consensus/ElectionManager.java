package com.dfs.consensus;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.dfs.config.ClusterConfig;
import com.dfs.config.RpcClient;
import com.dfs.model.RaftState;

/**
 * Drives Raft leader elections. Port of features/consensus/election.py.
 *
 * A background monitor loop watches the election timeout; when it fires the
 * node becomes a candidate and requests votes from all peers in parallel. A
 * majority promotes it to leader.
 */
@Component
public class ElectionManager {

    private static final Logger log = LoggerFactory.getLogger(ElectionManager.class);

    private static final Duration VOTE_TIMEOUT = Duration.ofMillis(200);

    private final RaftNode raft;
    private final RpcClient rpc;
    private final ClusterConfig config;
    private final ExecutorService executor;

    private volatile boolean running = false;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private Thread monitorThread;

    public ElectionManager(RaftNode raft, RpcClient rpc, ClusterConfig config,
                           @Qualifier("taskExecutor") ExecutorService executor) {
        this.raft = raft;
        this.rpc = rpc;
        this.config = config;
        this.executor = executor;
    }

    public void start() {
        running = true;
        monitorThread = Thread.ofVirtual().name("raft-election-" + raft.getNodeId()).start(this::monitorLoop);
        log.info("Election monitor started for {}", raft.getNodeId());
    }

    public void stop() {
        running = false;
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
    }

}
