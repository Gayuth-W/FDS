package com.dfs.model;

import java.util.List;

/**
 * File-level metadata tracked by the metadata repository.
 * Port of shared/models.py FileMetadata. Status is READY / DEGRADED / RECOVERING.
 */
public record FileMetadata(
        String fileId,
        String filename,
        int version,
        String primaryNode,
        List<String> replicaNodes,
        String status
) {
    public FileMetadata {
        if (replicaNodes == null) {
            replicaNodes = List.of();
        }
        if (status == null) {
            status = "READY";
        }
    }
}
