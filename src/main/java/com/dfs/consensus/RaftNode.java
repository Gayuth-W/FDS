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


    // ----- Queries / helpers -----

    public String getCurrentLeader() {
        lock.lock();
        try {
            return state == RaftState.LEADER ? nodeId : leaderId;
        } finally {
            lock.unlock();
        }
    }

    public boolean shouldStartElection() {
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                return false;
            }
            double elapsed = mono() - lastHeartbeat;
            return elapsed > electionTimeout;
        } finally {
            lock.unlock();
        }
    }

    public void resetElectionTimeout() {
        lock.lock();
        try {
            lastHeartbeat = mono();
            // Stable range after first contact: [1.5, 3.0]s.
            electionTimeout = ThreadLocalRandom.current().nextDouble(1500, 3000) / 1000.0;
        } finally {
            lock.unlock();
        }
    }

    public void registerCommitCallback(CommitCallback cb) {
        lock.lock();
        try {
            commitCallbacks.add(cb);
        } finally {
            lock.unlock();
        }
    }

    public void stop() {
        log.info("Raft node {} stopping", nodeId);
    }

    // ----- Accessors used by managers / status endpoints -----

    public String getNodeId() {
        return nodeId;
    }

    public List<String> getPeerIds() {
        return peerIds;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public RaftState getState() {
        return state;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public Map<String, Integer> getNextIndex() {
        return nextIndex;
    }

    public Map<String, Integer> getMatchIndex() {
        return matchIndex;
    }

    /** Term of the last log entry (0 if empty). */
    public int lastLogTerm() {
        return logEntries.isEmpty() ? 0 : logEntries.get(logEntries.size() - 1).getTerm();
    }

    public int lastLogIndex() {
        return logEntries.size();
    }
}
