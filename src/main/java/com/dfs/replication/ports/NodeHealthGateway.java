package com.dfs.replication.ports;

import com.dfs.model.Heartbeat;

import java.util.List;

/**
 * Port for node liveness. Mirrors the NodeHealthGateway Protocol in
 * shared/interfaces.py.
 */
public interface NodeHealthGateway {

    Heartbeat heartbeat(String nodeId);

    List<String> listLiveNodes();
}
