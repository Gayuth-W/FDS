package com.dfs.consensus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.dfs.config.ClusterConfig;
import com.dfs.model.LogEntry;
import com.dfs.model.RaftState;

import jakarta.annotation.PostConstruct;

/**
 * Raft consensus node state machine. Port of features/consensus/raft_node.py.
 *
 * Raft is used ONLY for metadata consensus (file manifests + leadership).
 *
 * Concurrency note: the original runs on a single asyncio event loop. Here we
 * have real threads (Tomcat request threads + background loops), so every piece
 * of mutable Raft state is guarded by {@link #lock}, a ReentrantLock. Network
 * I/O (RPC fan-out) is deliberately performed OUTSIDE the lock by the managers;
 * the short critical sections here keep state transitions atomic.
 */
@Component
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final ClusterConfig config;

    private String nodeId;
    private List<String> peerIds;

    // Persistent state
    private int currentTerm = 1;
    private String votedFor = null;
    private final List<LogEntry> logEntries = new ArrayList<>();

    // Volatile state
    private int commitIndex = 0;
    private int lastApplied = 0;
    private final List<CommitCallback> commitCallbacks = new ArrayList<>();

    // Leader state
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    // Node state
    private RaftState state = RaftState.FOLLOWER;
    private String leaderId = null;

    // Timing (monotonic seconds)
    private double electionTimeout;
    private double lastHeartbeat;

    /** Single lock guarding all Raft state. Exposed so managers can run compound critical sections. */
    public final ReentrantLock lock = new ReentrantLock();
}
