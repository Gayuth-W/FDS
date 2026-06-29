package com.dfs.model;

/**
 * Health state of a node as tracked by fault tolerance.
 * Port of the NodeStatus enum used in registry.py / manager.py.
 */
public enum NodeStatus {
    HEALTHY("healthy"),
    SUSPECTED("suspected"),
    FAILED("failed"),
    RECOVERING("recovering");

    private final String value;

    NodeStatus(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
