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

    public RaftNode(ClusterConfig config) {
        this.config = config;
    }

    @PostConstruct
    void init() {
        this.nodeId = config.getNodeId();
        this.peerIds = new ArrayList<>(config.getOtherNodes().keySet());

        // Wide, randomized initial timeout so the earliest node becomes candidate
        // and wins before any other node's timer fires (RPCs are ~ms, the window
        // is hundreds of ms), giving a decisive single-round first election.
        this.electionTimeout = ThreadLocalRandom.current().nextDouble(150, 800) / 1000.0;
        // Small initial jitter so nodes started together don't all fire at once.
        this.lastHeartbeat = mono() - ThreadLocalRandom.current().nextDouble(0, 0.2);

        log.info("[RAFT] Node {} initialized with peers {}", nodeId, peerIds);
    }

    private static double mono() {
        return System.nanoTime() / 1_000_000_000.0;
    }

    // ----- State transitions (acquire lock; reentrant-safe) -----

    public void becomeFollower(int term, String newLeaderId) {
        lock.lock();
        try {
            // Only reset votedFor when the term strictly advances.
            if (term > currentTerm) {
                votedFor = null;
            }
            state = RaftState.FOLLOWER;
            currentTerm = Math.max(currentTerm, term);
            leaderId = newLeaderId;
            lastHeartbeat = mono();
            electionTimeout = ThreadLocalRandom.current().nextDouble(300, 600) / 1000.0;
            log.info("[RAFT] Node {} became FOLLOWER (term {}, leader {})", nodeId, currentTerm, newLeaderId);
        } finally {
            lock.unlock();
        }
    }

    public void becomeCandidate() {
        lock.lock();
        try {
            state = RaftState.CANDIDATE;
            currentTerm += 1;
            votedFor = nodeId;
            lastHeartbeat = mono();
            electionTimeout = ThreadLocalRandom.current().nextDouble(300, 600) / 1000.0;
            log.info("[RAFT] Node {} became CANDIDATE for term {}", nodeId, currentTerm);
        } finally {
            lock.unlock();
        }
    }

    public void becomeLeader() {
        lock.lock();
        try {
            state = RaftState.LEADER;
            leaderId = nodeId;
            int lastLogIndex = logEntries.size();
            for (String peer : peerIds) {
                nextIndex.put(peer, lastLogIndex + 1);
                matchIndex.put(peer, 0);
            }
            log.info("[RAFT] Node {} became LEADER for term {}", nodeId, currentTerm);
        } finally {
            lock.unlock();
        }
    }

    // ----- Leader-only log operations -----

    /** Append an entry to the local log (leader only). */
    public boolean appendEntry(LogEntry entry) {
        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                return false;
            }
            entry.setTerm(currentTerm);
            entry.setIndex(logEntries.size() + 1);
            logEntries.add(entry);
            log.info("[RAFT] Leader appended entry {}", entry.getIndex());

            if (peerIds.isEmpty()) {
                commitIndex = logEntries.size();
                log.info("[RAFT] No peers, immediately committed up to index {}", commitIndex);
                applyCommittedEntries();
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Advance the commit index when a majority has replicated.
     * Returns true if the commit index advanced.
     */
    public boolean updateCommitIndex() {
        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                return false;
            }

            if (peerIds.isEmpty()) {
                if (logEntries.size() > commitIndex) {
                    commitIndex = logEntries.size();
                    log.info("[RAFT] No peers, committed up to index {}", commitIndex);
                }
            }

            List<Integer> matches = new ArrayList<>(matchIndex.values());
            matches.add(logEntries.size());
            matches.sort((a, b) -> Integer.compare(b, a)); // descending
            int majorityIndex = matches.get(matches.size() / 2);

            log.info("Update commit: matches={}, majority_index={}, current_commit={}",
                    matches, majorityIndex, commitIndex);

            if (majorityIndex > commitIndex) {
                if (majorityIndex > 0 && logEntries.get(majorityIndex - 1).getTerm() == currentTerm) {
                    commitIndex = majorityIndex;
                    log.info("[RAFT] COMMIT UPDATED to index {}", commitIndex);
                    applyCommittedEntries();
                    return true;
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** Apply newly committed entries to the state machine via callbacks. Caller may or may not hold the lock. */
    public void applyCommittedEntries() {
        lock.lock();
        try {
            while (lastApplied < commitIndex) {
                lastApplied += 1;
                LogEntry entry = logEntries.get(lastApplied - 1);
                log.info("[RAFT] [NODE {}] Applying committed entry {}: {}", nodeId, lastApplied, entry.getOp());
                for (CommitCallback cb : commitCallbacks) {
                    try {
                        cb.onCommit(entry);
                    } catch (Exception e) {
                        log.error("Error in Raft commit callback: {}", e.toString());
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

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
            // Stable range after contact with a leader: [1.0, 2.0]s. Heartbeats
            // arrive every ~50ms, so this tolerates many missed beats before a
            // follower challenges, yet keeps failover after a real leader loss fast.
            electionTimeout = ThreadLocalRandom.current().nextDouble(1000, 2000) / 1000.0;
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
