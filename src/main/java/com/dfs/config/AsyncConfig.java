package com.dfs.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Thread infrastructure for the node.
 *
 * The original FastAPI node uses a single asyncio event loop with many
 * concurrent coroutines (election monitor, heartbeat sender, replication
 * queue worker, failure monitor, checkpointer, fire-and-forget tasks).
 *
 * Java has real threads, so we map those coroutines onto virtual threads:
 * cheap, blocking-friendly, and a natural fit for the short-lived RPC fan-outs
 * (e.g. requesting votes from all peers in parallel).
 */
@Configuration
public class AsyncConfig {

    /**
     * General-purpose virtual-thread pool used for fire-and-forget work such as
     * parallel RPC fan-out and asynchronous recovery, analogous to
     * asyncio.create_task / asyncio.gather.
     */
    @Bean(name = "taskExecutor", destroyMethod = "shutdown")
    public ExecutorService taskExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
