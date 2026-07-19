package com.dfs.replication.ports;

import com.dfs.config.ClusterConfig;
import com.dfs.config.RpcClient;
import com.dfs.model.WriteRequest;
import com.dfs.util.HashUtil;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * HTTP implementation of {@link StorageDataGateway}. Port of
 * features/replication/storage_data_impl.py.
 *
 * Stores a block on a peer via POST /replicate and reads it back via
 * GET /files/{id} (returned hex-encoded, matching the original).
 */
@Component
public class StorageDataGatewayImpl implements StorageDataGateway {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private final ClusterConfig config;
    private final RpcClient rpc;

    public StorageDataGatewayImpl(ClusterConfig config, RpcClient rpc) {
        this.config = config;
        this.rpc = rpc;
    }

    @Override
    public boolean storeOnNode(String nodeId, WriteRequest req) {
        String peerUrl = config.urlFor(nodeId);
        if (peerUrl == null) {
            return false;
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("block_id", req.fileId());
        body.put("data", req.content());
        body.put("source_node", config.getNodeId());
        return rpc.postOk(peerUrl + "/replicate", body, TIMEOUT);
    }

    @Override
    public Optional<String> readFromNode(String nodeId, String fileId) {
        String peerUrl = config.urlFor(nodeId);
        if (peerUrl == null) {
            return Optional.empty();
        }
        Optional<byte[]> bytes = rpc.getBytes(peerUrl + "/files/" + fileId, Duration.ofSeconds(5));
        return bytes.map(HashUtil::toHex);
    }

    @Override
    public boolean deleteFromNode(String nodeId, String fileId) {
        // Optional in the original implementation; deletion is coordinated by the leader.
        return true;
    }
}
