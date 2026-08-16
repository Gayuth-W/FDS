package com.dfs.timesync;

import org.springframework.stereotype.Component;

/**
 * Lamport Logical Clock to maintain causality of events.
 * Port of features/time_sync/logical_clocks.py LamportClock.
 *
 * Physical time (OS / NTP) is used only for logging/debugging; logical
 * correctness is ensured by this clock, independent of wall-clock time.
 *
 * Registered as a singleton component so every layer shares one causal clock,
 * mirroring app.state.lamport_clock in the original.
 */
@Component
public class LamportClock {

    private long time = 0;
    private final Object lock = new Object();

    public long getTime() {
        synchronized (lock) {
            return time;
        }
    }

    /** Increment before a local event or before sending a message. */
    public long tick() {
        synchronized (lock) {
            time += 1;
            return time;
        }
    }

    /** Update on receiving a message carrying a remote timestamp. */
    public long update(long receivedTime) {
        synchronized (lock) {
            time = Math.max(time, receivedTime) + 1;
            return time;
        }
    }
}
