package com.dfs.consensus;

import java.time.Duration;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.dfs.config.ClusterConfig;
import com.dfs.config.RpcClient;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Replicates committed log entries to followers and drives leader heartbeats.
 * Port of features/consensus/log_replication.py.
 *
 * The leader runs a tight heartbeat loop (AppendEntries with no entries). Real
 * entries are pushed via {@link #replicateLog}, which appends locally, fans the
 * entry out, and blocks until the entry is committed (majority match) or times out.
 * 
 * 
 *                LEADER
                    |
        -------------------------
        |           |           |
     Follower1   Follower2   Follower3

            AppendEntries RPC
                    |
         LogReplicationManager
 */
@Component
public class LogReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(LogReplicationManager.class);

    private static final Duration APPEND_TIMEOUT = Duration.ofMillis(200);
    private static final double COMMIT_TIMEOUT_SECONDS = 3.0;

    private final RaftNode raft;
    private final RpcClient rpc;
    private final ClusterConfig config;
    private final ObjectMapper mapper;
    private final ExecutorService executor;

    private volatile boolean running = false;
    private Thread heartbeatThread;

    public LogReplicationManager(RaftNode raft, RpcClient rpc, ClusterConfig config,
                                 ObjectMapper mapper,
                                 @Qualifier("taskExecutor") ExecutorService executor) {
        this.raft = raft;
        this.rpc = rpc;
        this.config = config;
        this.mapper = mapper;
        this.executor = executor;
    }


    private static double mono() {
        return System.nanoTime() / 1_000_000_000.0;
    }
}
