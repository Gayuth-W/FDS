package com.dfs.replication;

import com.dfs.config.ClusterConfig;
import org.springframework.stereotype.Component;

/**
 * Write-quorum rules. Port of features/replication/consistency.py.
 *
 * Basic majority rule: a write needs floor(RF/2)+1 acknowledgements to be
 * considered durable.
 */
@Component
public class QuorumService {

    /** Majority quorum for the default replication factor. */
    public int writeQuorumRequired() {
        return writeQuorumRequired(ClusterConfig.REPLICATION_FACTOR);
    }

    /** Majority quorum for an explicit replication factor. */
    public int writeQuorumRequired(int replicationFactor) {
        return (replicationFactor / 2) + 1;
    }

    public boolean isWriteSuccessful(int ackCount, int requiredAcks) {
        return ackCount >= requiredAcks;
    }

    /**
     * Reporting helper. Returns FULLY_REPLICATED / QUORUM_COMMITTED / FAILED.
     */
    public String classifyWriteState(int ackCount, int replicationFactor) {
        if (ackCount == replicationFactor) {
            return "FULLY_REPLICATED";
        }
        if (ackCount >= writeQuorumRequired(replicationFactor)) {
            return "QUORUM_COMMITTED";
        }
        return "FAILED";
    }
}
