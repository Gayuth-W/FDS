package com.dfs.replication.ports;

import com.dfs.model.FileMetadata;

import java.util.List;
import java.util.Optional;

/**
 * Port for file metadata storage. Mirrors the MetadataRepo Protocol in
 * shared/interfaces.py.
 */
public interface MetadataRepo {

    Optional<FileMetadata> getFileMetadata(String fileId);

    FileMetadata upsertFileMetadata(FileMetadata meta);

    List<String> chooseReplicaNodes(String fileId, int replicationFactor);
}
