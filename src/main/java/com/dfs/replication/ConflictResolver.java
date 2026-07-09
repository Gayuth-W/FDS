package com.dfs.replication;

import org.springframework.stereotype.Component;

/**
 * Deterministic conflict resolution. Port of features/replication/conflicts.py.
 *
 * Rule:
 *   - Higher Lamport timestamp wins.
 *   - On a tie, the lexicographically larger node id wins.
 *
 * This guarantees every node converges to the same winner without coordination.
 */
@Component
public class ConflictResolver {

    /**
     * @return true if the incoming value should overwrite the local value.
     */
    public boolean resolveConflict(long localLamport, long incomingLamport,
                                   String localNodeId, String incomingNodeId) {
        if (incomingLamport > localLamport) {
            return true;
        }
        if (incomingLamport == localLamport) {
            return incomingNodeId.compareTo(localNodeId) > 0;
        }
        return false;
    }

    /**
     * Monotonically increasing version for a new write. Used by the quorum write
     * path (Replicator) to stamp each successful write.
     */
    public int nextVersionForWrite(int currentVersion) {
        return currentVersion + 1;
    }
}
