package com.dfs.replication.ports;

import com.dfs.model.WriteRequest;

import java.util.Optional;

/**
 * Port for storing/reading block data on remote nodes. Mirrors the
 * StorageDataGateway Protocol in shared/interfaces.py.
 */
public interface StorageDataGateway {

    boolean storeOnNode(String nodeId, WriteRequest req);

    /** @return hex-encoded content, or empty if unavailable. */
    Optional<String> readFromNode(String nodeId, String fileId);

    boolean deleteFromNode(String nodeId, String fileId);
}
