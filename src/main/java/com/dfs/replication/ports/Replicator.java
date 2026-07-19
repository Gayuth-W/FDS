package com.dfs.replication.ports;

import com.dfs.config.ClusterConfig;
import com.dfs.model.FileMetadata;
import com.dfs.model.ReplicationResult;
import com.dfs.model.WriteRequest;
import com.dfs.model.WriteResult;
import com.dfs.replication.ConflictResolver;
import com.dfs.replication.QuorumService;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Coordinator-side quorum write path. Port of
 * features/replication/replicator.py.
 *
 * This is the hexagonal/"ports &amp; adapters" write flow: choose live replica
 * nodes, fan out the write, and only commit metadata if a write quorum acks.
 * It is kept for architectural parity with the original; the live FastAPI
 * upload path used the async replication pipeline instead, and the same is true
 * of the Spring upload controller here.
 */
@Component
public class Replicator {

    private final ClusterConfig config;
    private final MetadataRepo metadataRepo;
    private final NodeHealthGateway nodeHealthGateway;
    private final StorageDataGateway storageDataGateway;
    private final QuorumService quorum;
    private final ConflictResolver conflicts;

    public Replicator(ClusterConfig config,
            MetadataRepo metadataRepo,
            NodeHealthGateway nodeHealthGateway,
            StorageDataGateway storageDataGateway,
            QuorumService quorum,
            ConflictResolver conflicts) {
        this.config = config;
        this.metadataRepo = metadataRepo;
        this.nodeHealthGateway = nodeHealthGateway;
        this.storageDataGateway = storageDataGateway;
        this.quorum = quorum;
        this.conflicts = conflicts;
    }

    public List<String> chooseValidReplicaNodes(String fileId, int requestedFactor) {
        List<String> candidates = metadataRepo.chooseReplicaNodes(fileId, requestedFactor);
        Set<String> live = new HashSet<>(nodeHealthGateway.listLiveNodes());
        List<String> valid = new ArrayList<>();
        for (String nodeId : candidates) {
            if (live.contains(nodeId)) {
                valid.add(nodeId);
            }
        }
        return valid;
    }

    public ReplicationResult replicateToNodes(WriteRequest req, List<String> nodeIds, int version) {
        List<String> succeeded = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        for (String nodeId : nodeIds) {
            if (storageDataGateway.storeOnNode(nodeId, req)) {
                succeeded.add(nodeId);
            } else {
                failed.add(nodeId);
            }
        }
        return new ReplicationResult(nodeIds, succeeded, failed, succeeded.size());
    }

    public WriteResult handleWrite(WriteRequest req, String leaderId) {
        Optional<FileMetadata> currentMeta = metadataRepo.getFileMetadata(req.fileId());
        int currentVersion = currentMeta.map(FileMetadata::version).orElse(0);
        int newVersion = conflicts.nextVersionForWrite(currentVersion);

        List<String> replicaNodes = chooseValidReplicaNodes(req.fileId(), 3);
        if (replicaNodes.isEmpty()) {
            return new WriteResult(false, currentVersion, List.of(), leaderId);
        }

        ReplicationResult replication = replicateToNodes(req, replicaNodes, newVersion);
        int requiredAcks = quorum.writeQuorumRequired(ClusterConfig.REPLICATION_FACTOR);
        boolean success = quorum.isWriteSuccessful(replication.ackCount(), requiredAcks);

        if (!success) {
            return new WriteResult(false, currentVersion, replication.succeededNodes(), leaderId);
        }

        FileMetadata updated = new FileMetadata(
                req.fileId(),
                req.filename(),
                newVersion,
                replication.succeededNodes().get(0),
                replication.succeededNodes(),
                replication.failedNodes().isEmpty() ? "READY" : "DEGRADED");
        metadataRepo.upsertFileMetadata(updated);

        return new WriteResult(true, newVersion, replication.succeededNodes(), leaderId);
    }

    /** Default leader id used by the original when none is supplied. */
    public WriteResult handleWrite(WriteRequest req) {
        return handleWrite(req, "COORDINATOR_1");
    }
}
