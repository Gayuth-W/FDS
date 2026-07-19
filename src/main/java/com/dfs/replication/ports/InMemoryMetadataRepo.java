package com.dfs.replication.ports;

import com.dfs.config.ClusterConfig;
import com.dfs.model.FileMetadata;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory metadata repository. Port of
 * features/replication/metadata_impl.py.
 *
 * Fixed placement policy: the first {@code replicationFactor} nodes of the
 * cluster are chosen as replicas.
 */
@Component
public class InMemoryMetadataRepo implements MetadataRepo {

    private final ConcurrentHashMap<String, FileMetadata> files = new ConcurrentHashMap<>();
    private final List<String> defaultNodes;

    public InMemoryMetadataRepo(ClusterConfig config) {
        this.defaultNodes = new ArrayList<>(config.getAllNodes().keySet());
    }

    @Override
    public Optional<FileMetadata> getFileMetadata(String fileId) {
        return Optional.ofNullable(files.get(fileId));
    }

    @Override
    public FileMetadata upsertFileMetadata(FileMetadata meta) {
        files.put(meta.fileId(), meta);
        return meta;
    }

    @Override
    public List<String> chooseReplicaNodes(String fileId, int replicationFactor) {
        int n = Math.min(replicationFactor, defaultNodes.size());
        return new ArrayList<>(defaultNodes.subList(0, n));
    }
}
