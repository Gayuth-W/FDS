package com.dfs.controller;

import com.dfs.config.ClusterConfig;
import com.dfs.replication.StorageManager;
import com.dfs.util.HashUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Latency benchmark endpoints. Port of the /benchmark routes in main.py.
 *
 * The original referenced a module-level BENCHMARK_LOGS list; here it is a
 * thread-safe in-memory list so the endpoints function under concurrent load.
 */
@RestController
public class BenchmarkController {

    private final ClusterConfig config;
    private final StorageManager storage;
    private final List<Map<String, Object>> benchmarkLogs = new CopyOnWriteArrayList<>();

    public BenchmarkController(ClusterConfig config, StorageManager storage) {
        this.config = config;
        this.storage = storage;
    }

    @PostMapping("/benchmark/upload_latency")
    public Map<String, Object> benchmarkUpload(@RequestParam("file") MultipartFile file) throws Exception {
        double start = HashUtil.now();

        String filename = "bench_" + file.getOriginalFilename();
        byte[] content = file.getBytes();
        storage.writeBlock(filename, content);

        double latency = (HashUtil.now() - start) * 1000.0;

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("type", "upload");
        entry.put("latency_ms", latency);
        entry.put("size", content.length);
        entry.put("timestamp", HashUtil.now());
        entry.put("node", config.getNodeId());
        benchmarkLogs.add(entry);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("latency_ms", latency);
        body.put("size", content.length);
        return body;
    }

    @GetMapping("/api/benchmarks")
    public Map<String, Object> getBenchmarks() {
        return Map.of("data", benchmarkLogs);
    }
}
