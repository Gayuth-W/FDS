package com.dfs.replication.ports;

import com.dfs.config.ClusterConfig;
import com.dfs.faulttolerance.NodeRegistry;
import com.dfs.model.Heartbeat;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Node health gateway backed by the {@link NodeRegistry}. Port of
 * features/fault_tolerance/node_health_impl.py.
 *
 * The original was async; here liveness is read synchronously from the
 * registry, which is already thread-safe.
 */
@Component
public class NodeHealthGatewayImpl implements NodeHealthGateway {

    private final ClusterConfig config;
    private final NodeRegistry registry;

    public NodeHealthGatewayImpl(ClusterConfig config, NodeRegistry registry) {
        this.config = config;
        this.registry = registry;
    }

    @Override
    public Heartbeat heartbeat(String nodeId) {
        List<String> live = registry.getLiveNodes();
        boolean isAlive = live.contains(nodeId) || nodeId.equals(config.getNodeId());
        return new Heartbeat(nodeId, isAlive, System.currentTimeMillis() / 1000.0);
    }

    @Override
    public List<String> listLiveNodes() {
        return registry.getLiveNodes();
    }
}
