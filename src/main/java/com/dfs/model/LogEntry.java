package com.dfs.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single Raft log entry. Port of shared/models.py LogEntry.
 *
 * Raft is used ONLY for metadata consensus (file manifests + leadership), so
 * {@code op} is typically "CREATE_FILE" and {@code payload} is the file manifest.
 *
 * term/index are mutable because the leader stamps them at append time
 * (RaftNode.append_entry), matching the original behaviour.
 */
public class LogEntry {

    private int term;
    private int index;
    private String op;
    private String fileId;
    private Map<String, Object> payload;

    public LogEntry() {
    }

    public LogEntry(int term, int index, String op, String fileId, Map<String, Object> payload) {
        this.term = term;
        this.index = index;
        this.op = op;
        this.fileId = fileId;
        this.payload = payload;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    /** Serialize to the JSON wire shape used by /raft/append_entries. */
    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("term", term);
        m.put("index", index);
        m.put("op", op);
        m.put("file_id", fileId);
        m.put("payload", payload);
        return m;
    }

    /** Rebuild from a JSON node received over the wire. */
    @SuppressWarnings("unchecked")
    public static LogEntry fromJson(JsonNode node, ObjectMapper mapper) {
        LogEntry e = new LogEntry();
        e.term = node.path("term").asInt(0);
        e.index = node.path("index").asInt(0);
        e.op = node.path("op").asText(null);
        e.fileId = node.path("file_id").asText(null);
        JsonNode payloadNode = node.get("payload");
        if (payloadNode != null && !payloadNode.isNull()) {
            e.payload = mapper.convertValue(payloadNode, Map.class);
        } else {
            e.payload = new LinkedHashMap<>();
        }
        return e;
    }
}
