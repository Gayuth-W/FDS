package com.dfs.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Central cluster configuration. Port of shared/config.py.
 *
 * NODE_ID and PORT come from environment variables (defaults node1 / 8001).
 * The full node->URL map is fixed for a 5-node local cluster, matching the
 * original configuration.
 */
@Component
public class ClusterConfig {

    private static final Logger log = LoggerFactory.getLogger(ClusterConfig.class);

    public static final String HOST = "127.0.0.1";

    /** Replication factor for the cluster (matches original REPLICATION_FACTOR = 5). */
    public static final int REPLICATION_FACTOR = 5;

    /** Fixed cluster membership: nodeId -> base URL. Insertion order preserved. */
    private final Map<String, String> allNodes = new LinkedHashMap<>();

    private final String nodeId;
    private final int port;
    private final Path baseDir;

    public ClusterConfig() {
        // Cluster membership. By default this is the fixed local 5-node map
        // (127.0.0.1:8001..8005), matching the original single-host layout.
        //
        // For containerized runs, peers live in other network namespaces and
        // must be addressed by hostname, so the whole map can be overridden with
        // a CLUSTER_NODES env var of the form:
        //   node1=http://node1:8001,node2=http://node2:8002,...
        // This is the only seam Docker needs; local behaviour is unchanged when
        // the variable is unset.
        String clusterNodes = envOrDefault("CLUSTER_NODES", null);
        if (clusterNodes != null) {
            for (String pair : clusterNodes.split(",")) {
                String entry = pair.trim();
                if (entry.isEmpty()) {
                    continue;
                }
                int eq = entry.indexOf('=');
                if (eq <= 0) {
                    throw new IllegalArgumentException(
                            "Invalid CLUSTER_NODES entry (expected id=url): " + entry);
                }
                allNodes.put(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
            }
        } else {
            allNodes.put("node1", "http://127.0.0.1:8001");
            allNodes.put("node2", "http://127.0.0.1:8002");
            allNodes.put("node3", "http://127.0.0.1:8003");
            allNodes.put("node4", "http://127.0.0.1:8004");
            allNodes.put("node5", "http://127.0.0.1:8005");
        }

        this.nodeId = envOrDefault("NODE_ID", "node1");
        this.port = Integer.parseInt(envOrDefault("PORT", "8001"));

        // Anchor data directories to the working directory of the process,
        // partitioned per node (data/<nodeId>/...). Mirrors the Python layout.
        this.baseDir = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    }

    @PostConstruct
    void announce() {
        log.info("ClusterConfig ready: nodeId={}, port={}, baseDir={}", nodeId, port, baseDir);
        // Log the resolved peer map so misconfiguration is obvious: in Docker these
        // must be service names (http://node2:8002), NOT http://127.0.0.1:8002.
        log.info("Cluster membership: {}", allNodes);
        log.info("This node's peers: {}", getOtherNodes());
    }

    private static String envOrDefault(String key, String def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            // Allow -D override as a convenience as well.
            v = System.getProperty(key);
        }
        return (v == null || v.isBlank()) ? def : v;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getPort() {
        return port;
    }

    public Map<String, String> getAllNodes() {
        return allNodes;
    }

    /** All nodes except this one: peerId -> URL. */
    public Map<String, String> getOtherNodes() {
        return allNodes.entrySet().stream()
                .filter(e -> !e.getKey().equals(nodeId))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (a, b) -> a, LinkedHashMap::new));
    }

    public String urlFor(String node) {
        return allNodes.get(node);
    }

    public Path getDataDir() {
        return baseDir.resolve("data").resolve(nodeId);
    }

    public Path getBlocksDir() {
        return getDataDir().resolve("blocks");
    }

    public Path getCheckpointDir() {
        return getDataDir().resolve("checkpoints");
    }
}
