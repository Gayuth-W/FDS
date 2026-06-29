package com.dfs.model;

/** Raft role for a node. Mirrors the string states ("follower"/"candidate"/"leader"). */
public enum RaftState {
    FOLLOWER("follower"),
    CANDIDATE("candidate"),
    LEADER("leader");

    private final String wire;

    RaftState(String wire) {
        this.wire = wire;
    }

    /** Lower-case wire form, matching the original status/metrics JSON. */
    public String wire() {
        return wire;
    }
}
