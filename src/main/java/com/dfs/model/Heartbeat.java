package com.dfs.model;

/** Heartbeat status for a node. Port of shared/models.py Heartbeat. */
public record Heartbeat(
        String nodeId,
        boolean isAlive,
        double timestamp
) {
}
