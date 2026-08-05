# FDS — a fault-tested, linearizable replicated store on Raft

A 5-node distributed file store (Java 21 / Spring Boot) whose **consistency is
verified under fault injection**. Raft provides leader election and metadata
consensus; file blocks are replicated across nodes; a chaos harness partitions,
crashes, and stalls the cluster while asserting that no acknowledged write is
ever lost and that reads are linearizable.

This is a hardened Java port of an earlier Python/FastAPI prototype. The port
kept the four-layer design, cut the scaffolding that wasn't load-bearing, fixed
two real consistency bugs, and added the harness that proves the fixes.

## What it guarantees (and how it's checked)

The `harness/` suite drives a workload against the live cluster under faults and
verifies four invariants — see [`harness/README.md`](harness/README.md):

- **No split-brain** — never two leaders in one term.
- **No acknowledged-write loss** — every acked write reads back byte-identical
  after crashes/partitions.
- **Quorum safety** — no write is acked while a majority is unavailable.
- **Read-your-writes** — a just-acked write never reads back stale.

```bash
docker compose up --build          # start the cluster + dashboard + Prometheus
cd harness && pip install -r requirements.txt
python run_chaos.py --scenarios 12 --seed 7
python loadtest.py --clients 16 --duration 20
```

## Two consistency bugs this fixes

1. **Stale reads.** Reads could be served from a node whose manifest hadn't yet
   been replicated, returning stale/absent data. **Fix:** reads are served by the
   leader — any node forwards the read to the current leader, so a client always
   observes committed state. (Scoped note: this routes to whoever the node
   believes is leader; a read-index — confirming leadership via a heartbeat round
   before serving — is the natural next hardening step and is called out in the
   code.)
2. **Deletes bypassing consensus.** Deletes were applied leader-locally and never
   went through Raft, so a node that missed the delete could resurrect the file.
   **Fix:** deletes are committed as a `DELETE_FILE` Raft entry and applied via the
   commit callback on **every** node, so a deleted file cannot reappear.

## Architecture

- **Consensus (Raft).** Leader election + replicated log for **metadata only**
  (file manifests, leadership). Single-writer coordination through the leader.
- **Replication.** File content is split into 1 MB blocks and replicated to peers;
  manifests are committed through Raft so every node agrees on file metadata.
- **Fault tolerance.** Heartbeat-based failure detection, JSON checkpointing, and
  background recovery of missing blocks when a node rejoins.
- **Ordering.** Lamport clocks provide last-writer-wins ordering independent of
  wall-clock time (which is why the store tolerates clock skew).

Runs as **5 identical containers** — same image, identity via `NODE_ID` / `PORT`,
peers discovered by service name via `CLUSTER_NODES`.

## Running it

| Component | URL |
|-----------|-----|
| React dashboard | http://localhost:8501 |
| Prometheus (scrapes every node) | http://localhost:9090 |
| Nodes | http://localhost:8001 … :8005 |

Each node exposes Prometheus metrics at `/actuator/prometheus`, including
`dfs_file_upload_latency` (write-path percentiles). The dashboard shows the
cluster: leader, per-node Raft state, replication, failure detection, and file
upload/download/delete.

### Local dev (without Docker)

```bash
mvn -DskipTests package
# then run 5 instances with NODE_ID / PORT / CLUSTER_NODES set per node
cd ui && npm install && npm run dev      # dashboard on http://localhost:5173
```

## Layout

```
src/main/java/com/dfs/
  consensus/      Raft: election + log replication
  replication/    block storage + peer replication
  faulttolerance/ failure detection + recovery
  timesync/       Lamport clock
  controller/     REST endpoints (files, cluster, raft, replication)
  config/         bootstrap, cluster config, RPC client, CORS
harness/          chaos suite + load generator (the verification story)
ui/               React dashboard
observability/    Prometheus scrape config
```

## Honest scope

Reads are leader-routed (strong in the common case; a read-index would close the
partitioned-stale-leader edge). Throughput is a Raft-quorum write path, so it is
measured in thousands/sec — lower than a single-node in-memory engine by design,
and that's the correct expectation. All numbers in a write-up should come from
`loadtest.py` on your own machine so they're reproducible on request.
