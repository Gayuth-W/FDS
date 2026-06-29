package com.dfs.model;

/** A client write request in the quorum write path. Port of shared/models.py WriteRequest. */
public record WriteRequest(
        String fileId,
        String filename,
        String content,
        double clientTs
) {
}
