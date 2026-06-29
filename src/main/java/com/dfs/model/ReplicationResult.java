package com.dfs.model;

import java.util.List;

/** Outcome of replicating to a set of nodes. Port of shared/models.py ReplicationResult. */
public record ReplicationResult(
        List<String> attemptedNodes,
        List<String> succeededNodes,
        List<String> failedNodes,
        int ackCount
) {
    public ReplicationResult {
        if (attemptedNodes == null) attemptedNodes = List.of();
        if (succeededNodes == null) succeededNodes = List.of();
        if (failedNodes == null) failedNodes = List.of();
    }
}
