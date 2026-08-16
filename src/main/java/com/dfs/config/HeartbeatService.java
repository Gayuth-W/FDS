package com.dfs.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Background heartbeat sender. Port of heartbeat_sender() in main.py.
 *
 * Every ~1s this node POSTs {"node_id": ...} to each peer's /heartbeat endpoint
 * so peers can detect its liveness. Runs on a virtual thread, mirroring the
 * original asyncio loop.
 */
@Component
public class HeartbeatService {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);
    private static final Duration TIMEOUT = Duration.ofMillis(500);

    private final ClusterConfig config;
    private final RpcClient rpc;

    private volatile boolean running = true;
    private Thread thread;

    public HeartbeatService(ClusterConfig config, RpcClient rpc) {
        this.config = config;
        this.rpc = rpc;
    }

    @PostConstruct
    void start() {
        running = true;
        thread = Thread.ofVirtual().name("heartbeat-" + config.getNodeId()).start(this::loop);
        log.info("Heartbeat sender started for {}", config.getNodeId());
    }

    @PreDestroy
    void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private void loop() {
        while (running) {
            for (Map.Entry<String, String> e : config.getOtherNodes().entrySet()) {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("node_id", config.getNodeId());
                rpc.postOk(e.getValue() + "/heartbeat", body, TIMEOUT);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
