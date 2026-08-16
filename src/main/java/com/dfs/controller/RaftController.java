package com.dfs.controller;

import com.dfs.consensus.ConsensusService;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Raft RPC endpoints. Port of the /raft routes in main.py.
 *
 * Bodies are parsed as raw JSON so the entries list keeps its exact wire shape
 * (a list of manifest log entries) without binding to intermediate POJOs.
 */
@RestController
public class RaftController {

    private final ConsensusService consensus;

    public RaftController(ConsensusService consensus) {
        this.consensus = consensus;
    }

    @PostMapping("/raft/vote")
    public Map<String, Object> vote(@RequestBody JsonNode data) {
        return consensus.handleVoteRequest(
                data.path("term").asInt(),
                data.path("candidate_id").asText(null),
                data.path("last_log_index").asInt(),
                data.path("last_log_term").asInt());
    }

    @PostMapping("/raft/append_entries")
    public Map<String, Object> appendEntries(@RequestBody JsonNode data) {
        List<JsonNode> entries = new ArrayList<>();
        JsonNode entriesNode = data.get("entries");
        if (entriesNode != null && entriesNode.isArray()) {
            entriesNode.forEach(entries::add);
        }
        return consensus.handleAppendEntries(
                data.path("term").asInt(),
                data.path("leader_id").asText(null),
                data.path("prev_log_index").asInt(),
                data.path("prev_log_term").asInt(),
                entries,
                data.path("leader_commit").asInt());
    }
}
