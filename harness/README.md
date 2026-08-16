# Chaos harness & load generator

This directory contains the tooling that **verifies** the store's consistency
under fault injection, and measures its write path.

## What it proves

The harness drives a workload against the live 5-node cluster while injecting
faults, then checks four invariants:

| Invariant | What it rules out |
|-----------|-------------------|
| **No split-brain** | Two nodes acting as leader in the same term |
| **No acknowledged-write loss** | An acked write disappearing after a crash/partition |
| **Quorum safety** | A write being acked while a majority is unavailable |
| **Read-your-writes** | A just-acked write reading back stale or missing |

The guiding rule: we only ever assert on writes the cluster **positively
acknowledged**. If it said "committed", that write must survive every fault and
read back byte-for-byte. Writes refused during quorum loss are expected to be
absent and are never asserted on.

## Prerequisites

- The cluster running: `docker compose up --build` (from the repo root)
- Python 3.9+ and `pip install -r requirements.txt`
- The Docker CLI on PATH (the harness injects faults via `docker`)

## Run the chaos suite

```bash
pip install -r requirements.txt
python run_chaos.py --scenarios 8            # randomized scenarios
python run_chaos.py --scenarios 20 --seed 7  # reproducible run
```

Scenario types: crash the leader / a node, crash two nodes, partition a minority,
stall a node (`docker pause`), and **quorum loss** (three nodes down — writes must
be refused). The suite exits non-zero if any invariant is violated, so it works
in CI.

### Clock skew (optional)

Wall-clock skew is injected with `libfaketime`, which is **opt-in** so normal runs
are unaffected. Start the cluster with it enabled, then include the scenario:

```bash
ENABLE_FAKETIME=1 docker compose up --build
python run_chaos.py --scenarios 12 --clock-skew
```

This store orders writes with Lamport clocks and measures liveness with
receiver-side timers, so it is *designed* to be insensitive to wall-clock skew —
the scenario demonstrates that the invariants still hold when clocks disagree.

## Load test

```bash
python loadtest.py --clients 16 --duration 20              # write throughput + p99
python loadtest.py --clients 24 --duration 30 --read-ratio 0.3
```

Latency percentiles are measured **client-side, end-to-end** (request issued →
ack). Those are the honest, reproducible figures to quote — writes go through a
Raft quorum round-trip, so expect thousands/sec, not the hundreds-of-thousands a
single-node in-memory engine reports.

## Files

- `cluster.py` — cluster client (leader detection, upload/download/delete)
- `workload.py` — records acknowledged writes; checks read-your-writes
- `faults.py` — crash / pause / partition / clock-skew via the Docker CLI
- `invariants.py` — the four invariant checkers
- `run_chaos.py` — orchestrates scenarios and prints the report
- `loadtest.py` — concurrent load generator
