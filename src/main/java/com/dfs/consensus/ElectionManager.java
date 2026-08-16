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

    // 200ms is too tight for the first container-to-container connection (TCP
    // setup + a peer whose Tomcat is still warming). A down peer still fails fast
    // (connection refused), so this only adds slack for slow-but-alive peers.
    private static final Duration VOTE_TIMEOUT = Duration.ofMillis(800);

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

    private void monitorLoop() {
        while (running) {
            try {
                Thread.sleep(30);

                if (!electionInProgress.get()
                        && raft.getState() != RaftState.LEADER
                        && raft.shouldStartElection()) {
                    log.info("ELECTION TRIGGERED for {}. Applying randomized backoff...", raft.getNodeId());
                    // Small randomized backoff before sending votes to avoid simultaneous candidacies.
                    Thread.sleep((long) (ThreadLocalRandom.current().nextDouble(0.02, 0.08) * 1000));
                    executor.submit(this::startElection);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Election loop error on {}: {}", raft.getNodeId(), e.toString());
                try {
                    Thread.sleep(1000); // backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    public void startElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            raft.becomeCandidate();
            int currentTerm = raft.getCurrentTerm();
            log.info("Node {} starting election for term {}", raft.getNodeId(), currentTerm);

            // Request votes from all peers in parallel.
            List<String> peers = raft.getPeerIds();
            List<Future<VoteResult>> futures = peers.stream()
                    .map(peer -> executor.submit(() -> requestVote(peer)))
                    .toList();

            if (raft.getState() != RaftState.CANDIDATE) {
                return;
            }

            int votes = 1; // self vote
            for (Future<VoteResult> f : futures) {
                try {
                    VoteResult result = f.get();
                    if (result.term() > currentTerm) {
                        raft.becomeFollower(result.term(), null);
                        return;
                    }
                    if (result.voteGranted()) {
                        votes++;
                    }
                } catch (Exception e) {
                    log.error("Vote request failed: {}", e.toString());
                }
            }

            int totalNodes = peers.size() + 1;
            int majority = totalNodes / 2 + 1;
            log.info("Node {} received {}/{} votes", raft.getNodeId(), votes, majority);

            if (votes >= majority && raft.getState() == RaftState.CANDIDATE) {
                raft.becomeLeader();
                raft.resetElectionTimeout();
                log.info("Node {} is now LEADER for term {}", raft.getNodeId(), raft.getCurrentTerm());
            } else {
                log.info("Node {} failed to win election (votes={}/{})", raft.getNodeId(), votes, majority);
            }
        } finally {
            electionInProgress.set(false);
        }
    }

    private VoteResult requestVote(String peerId) {
        String peerUrl = config.urlFor(peerId);
        if (peerUrl == null) {
            return new VoteResult(0, false);
        }

        int lastLogIndex = raft.lastLogIndex();
        int lastLogTerm = raft.lastLogTerm();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("term", raft.getCurrentTerm());
        body.put("candidate_id", raft.getNodeId());
        body.put("last_log_index", lastLogIndex);
        body.put("last_log_term", lastLogTerm);

        return rpc.postJson(peerUrl + "/raft/vote", body, VOTE_TIMEOUT)
                .map(node -> new VoteResult(node.path("term").asInt(0), node.path("vote_granted").asBoolean(false)))
                .orElse(new VoteResult(0, false));
    }

    /**
     * Handle an incoming RequestVote RPC. Acquires the Raft lock for the whole
     * decision so the term check, vote-grant and timeout reset are atomic.
     */
    public Map<String, Object> handleVoteRequest(int term, String candidateId,
                                                 int lastLogIndex, int lastLogTerm) {
        raft.lock.lock();
        try {
            if (term > raft.getCurrentTerm()) {
                log.info("Term higher than current ({} > {}), stepping down.", term, raft.getCurrentTerm());
                raft.becomeFollower(term, null);
            }

            boolean voteGranted = false;
            String reason = "Unknown";

            if (term < raft.getCurrentTerm()) {
                reason = "Term out of date (" + term + " < " + raft.getCurrentTerm() + ")";
            } else if (term == raft.getCurrentTerm()) {
                if (raft.getVotedFor() != null && !raft.getVotedFor().equals(candidateId)) {
                    reason = "Already voted for " + raft.getVotedFor();
                } else {
                    int lastLogTermSelf = raft.lastLogTerm();
                    int lastLogIndexSelf = raft.lastLogIndex();
                    if (lastLogTerm > lastLogTermSelf
                            || (lastLogTerm == lastLogTermSelf && lastLogIndex >= lastLogIndexSelf)) {
                        voteGranted = true;
                        raft.setVotedFor(candidateId);
                        raft.resetElectionTimeout();
                        log.info("{} voted for {} (term {})", raft.getNodeId(), candidateId, term);
                    } else {
                        reason = "Candidate log not up to date";
                    }
                }
            }

            if (!voteGranted) {
                log.info("{} denied vote to {}: {}", raft.getNodeId(), candidateId, reason);
            }

            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("term", raft.getCurrentTerm());
            resp.put("vote_granted", voteGranted);
            return resp;
        } finally {
            raft.lock.unlock();
        }
    }

    /** Result of a RequestVote RPC. */
    public record VoteResult(int term, boolean voteGranted) {
    }
}
