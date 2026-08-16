package com.dfs.timesync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Vector Clock to track causality and detect concurrent events.
 * Port of features/time_sync/logical_clocks.py VectorClock.
 *
 * Not a Spring bean: a vector clock is per-logical-stream and is created where
 * needed, matching the original (constructed with a node id).
 */
public class VectorClock {

    private final String nodeId;
    private final Map<String, Integer> vector = new HashMap<>();
    private final Object lock = new Object();

    public VectorClock(String nodeId) {
        this(nodeId, List.of(nodeId));
    }

    public VectorClock(String nodeId, List<String> initialNodes) {
        this.nodeId = nodeId;
        for (String n : initialNodes) {
            vector.put(n, 0);
        }
    }

    public Map<String, Integer> getVector() {
        synchronized (lock) {
            return new HashMap<>(vector);
        }
    }

    public void addNode(String node) {
        synchronized (lock) {
            vector.putIfAbsent(node, 0);
        }
    }

    /** Increment the local counter before a local event or before sending. */
    public Map<String, Integer> tick() {
        synchronized (lock) {
            vector.merge(nodeId, 1, Integer::sum);
            return new HashMap<>(vector);
        }
    }

    /** Merge a received vector and advance the local counter. */
    public Map<String, Integer> update(Map<String, Integer> received) {
        synchronized (lock) {
            vector.merge(nodeId, 1, Integer::sum);
            for (Map.Entry<String, Integer> e : received.entrySet()) {
                vector.merge(e.getKey(), e.getValue(), Math::max);
            }
            return new HashMap<>(vector);
        }
    }

    /**
     * Compare two vector clocks.
     * @return 1 if v1 happens-after v2, -1 if v1 happens-before v2,
     *         0 if equal, null if concurrent.
     */
    public static Integer compare(Map<String, Integer> v1, Map<String, Integer> v2) {
        Set<String> allNodes = new HashSet<>(v1.keySet());
        allNodes.addAll(v2.keySet());

        boolean v1Greater = false;
        boolean v2Greater = false;
        for (String node : allNodes) {
            int a = v1.getOrDefault(node, 0);
            int b = v2.getOrDefault(node, 0);
            if (a > b) {
                v1Greater = true;
            } else if (b > a) {
                v2Greater = true;
            }
        }

        if (v1Greater && !v2Greater) {
            return 1;
        } else if (v2Greater && !v1Greater) {
            return -1;
        } else if (!v1Greater) {
            return 0;
        } else {
            return null; // concurrent
        }
    }
}
