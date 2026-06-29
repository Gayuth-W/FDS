package com.dfs.model;

import java.util.List;

/** Result of a quorum write. Port of shared/models.py WriteResult. */
public record WriteResult(
        boolean success,
        int version,
        List<String> committedNodes,
        String leaderId
) {
    public WriteResult {
        if (committedNodes == null) {
            committedNodes = List.of();
        }
    }
}
