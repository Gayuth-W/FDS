package com.dfs.consensus;

import com.dfs.model.LogEntry;

/**
 * Callback invoked when a Raft log entry is committed and applied to the
 * state machine. Port of the commit_callbacks mechanism in raft_node.py.
 */
@FunctionalInterface
public interface CommitCallback {
    void onCommit(LogEntry entry) throws Exception;
}
