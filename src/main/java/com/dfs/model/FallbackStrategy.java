package com.dfs.model;

/**
 * Actions the system can take when clocks drift too far.
 * Port of features/time_sync/fallback.py FallbackStrategy.
 */
public enum FallbackStrategy {
    PAUSE_WRITES("PAUSE_WRITES"),
    USE_COORDINATOR_TIME("USE_COORDINATOR_TIME"),
    WARN_ONLY("WARN_ONLY"),
    READ_ONLY_MODE("READ_ONLY_MODE");

    private final String value;

    FallbackStrategy(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /**
     * Escalation policy: the bigger the drift, the more aggressive the response.
     * Port of handle_time_sync_failure().
     */
    public static FallbackStrategy fromDrift(double maxAbsOffsetMs, int maxOffsetMs, FallbackStrategy current) {
        if (maxAbsOffsetMs > maxOffsetMs * 3) {
            return PAUSE_WRITES;
        } else if (maxAbsOffsetMs > maxOffsetMs * 2) {
            return USE_COORDINATOR_TIME;
        } else if (maxAbsOffsetMs > maxOffsetMs) {
            return WARN_ONLY;
        }
        return current != null ? current : WARN_ONLY;
    }
}
