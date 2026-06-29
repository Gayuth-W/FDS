package com.dfs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for a single Distributed File System node.
 *
 * Each node is an identical Spring Boot service. Identity (NODE_ID) and the
 * listening PORT are taken from environment variables, exactly like the
 * original FastAPI implementation (main.py --mode server --node-id nodeX --port 800X).
 *
 * Run a 5-node cluster by launching this same jar five times with different
 * NODE_ID / PORT values (see run-cluster scripts).
 */
@SpringBootApplication
@EnableScheduling
public class DfsApplication {

    public static void main(String[] args) {
        // PORT env var is resolved by application.yml (${PORT:8001}); nothing else needed.
        SpringApplication.run(DfsApplication.class, args);
    }
}
