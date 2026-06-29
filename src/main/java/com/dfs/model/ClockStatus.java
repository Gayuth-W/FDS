package com.dfs.model;

/** Per-node clock sync status. Port of shared/models.py ClockStatus. */
public record ClockStatus(
        String nodeId,
        int offsetMs,
        boolean inSync
) {
}
