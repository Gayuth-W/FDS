# Fault-Tolerant Distributed File System — Java / Spring Boot

The architecture is unchanged — only the web/runtime layer moved from FastAPI to
Spring Boot. The same four cooperating subsystems are present, the REST contracts
are byte-for-byte compatible, and the original **Streamlit dashboard (`dfs_ui.py`)
and Python CLI client work against this cluster without any changes**.

---

## What it does

Five identical nodes form a cluster. A file uploaded to the leader is split into
1 MB blocks, the blocks are replicated to peers, and the file *manifest* is
committed through **Raft** so every node agrees on metadata and on who the leader
is. Nodes heartbeat each other; a node that goes quiet is suspected, then failed,
and its data is re-replicated when it returns. Logical (Lamport) clocks order
events and break write conflicts deterministically.

### The four layers

| Layer | Responsibility | Key classes |
|-------|----------------|-------------|
| **Consensus (Raft)** | Leader election + metadata log replication | `RaftNode`, `ElectionManager`, `LogReplicationManager`, `ConsensusService` |
| **Data replication** | Block + manifest replication, quorum, conflicts, durable storage | `ReplicationManager`, `StorageManager`, `QuorumService`, `ConflictResolver`, `replication.ports.*` |
| **Fault tolerance** | Failure detection, checkpointing, recovery | `NodeRegistry`, `FaultToleranceManager`, `RecoveryManager` |
| **Time sync** | Lamport / vector clocks, drift fallback, monitoring | `LamportClock`, `VectorClock`, `TimeSyncMonitor`, `FallbackStrategy` |

Raft is used **only** for metadata consensus (file manifests + leadership).
Bulk block data travels on a separate asynchronous replication pipeline, exactly
as in the original.

---

## Prerequisites

- **JDK 21** (the code uses virtual threads and records — `java -version` should show 21)
- **Maven 3.9+** (or use the Maven wrapper / your IDE's bundled Maven)

---

## Build

```bash
mvn -DskipTests package
```

This produces a single runnable fat jar:

```
target/dfs-node.jar
```

One jar runs any node — identity comes from environment variables at launch.

---

## Run a 5-node cluster

Each node is the **same jar** started with a different `NODE_ID` and `PORT`
(node1→8001 … node5→8005), mirroring `python main.py --mode demo`.

### Windows / PowerShell

```powershell
# Build + launch all 5 nodes (each in its own window):
.\scripts\run-cluster.ps1

# If you've already built and just want to relaunch:
.\scripts\run-cluster.ps1 -SkipBuild

# Stop everything:
.\scripts\stop-cluster.ps1
```

Run a single node by hand:

```powershell
.\scripts\run-node.ps1 -NodeId node1 -Port 8001
```

### macOS / Linux

```bash
./scripts/run-cluster.sh          # logs to node_<id>.log, Ctrl+C stops all
```

### Fully manual (any OS)

```bash
# terminal 1
NODE_ID=node1 PORT=8001 java -jar target/dfs-node.jar
# terminal 2
NODE_ID=node2 PORT=8002 java -jar target/dfs-node.jar
# ... node3:8003, node4:8004, node5:8005
```

Give the cluster a few seconds to elect a leader, then hit any node's
`/status`. Data for each node lives under `data/<nodeId>/` (created on startup).

---

## Try it

```bash
# Find the leader (any node will report it)
curl http://127.0.0.1:8001/status

# Upload to the leader (replace 8001 with the leader's port if different).
# Non-leaders reply { "status":"error","message":"not leader","leader_url":... }
curl -F "file=@./hello.txt" http://127.0.0.1:8001/files/hello.txt

# Download
curl -O http://127.0.0.1:8001/files/hello.txt

# Delete (leader-only)
curl -X DELETE http://127.0.0.1:8001/files/hello.txt
```

### Using the original dashboard / client

The Streamlit UI and CLI from the Python repo are pure HTTP clients pointed at
`http://127.0.0.1:8001` (and peers), so they work unchanged:

```bash
streamlit run dfs_ui.py           # live metrics dashboard
python main.py --mode client      # interactive: write / read / status
```

---

## REST API (unchanged from the original)

| Method | Path | Notes |
|--------|------|-------|
| `POST` | `/files/{filename}` | Multipart `file`. Leader-only; splits, replicates, Raft-commits manifest |
| `GET` | `/files/{filename}` | Reassembles blocks → `application/octet-stream` |
| `DELETE` | `/files/{filename}` | Leader-only |
| `GET` | `/health` | `{status, node, state}` |
| `POST` | `/heartbeat` | `{node_id}` → `{status, time}` |
| `GET` | `/time` | `{node_id, time}` (Unix epoch seconds) |
| `POST` | `/raft/vote` | RequestVote RPC |
| `POST` | `/raft/append_entries` | AppendEntries RPC (heartbeats + log) |
| `POST` | `/replicate` | Receive a block (hex-encoded in JSON) |
| `POST` | `/replicate_meta` | Receive a manifest |
| `GET` | `/status`, `/api/status` | Full node/system status |
| `GET` | `/api/metrics` | Dashboard metrics (consensus / replication / fault / time) |
| `POST` | `/recovery/plan` | `{failed_node}` → recovery plan |
| `POST` | `/recovery/execute` | `{recovering_node, recovery_plan}` |
| `POST` | `/benchmark/upload_latency` | Multipart `file`; measures write latency |
| `GET` | `/api/benchmarks` | Collected benchmark samples |

---

## Project layout

```
src/main/java/com/dfs/
├── DfsApplication.java            # Spring Boot entry point (@EnableScheduling)
├── config/                        # ClusterConfig, RpcClient (JDK HttpClient),
│                                  #   AsyncConfig (virtual threads),
│                                  #   ClusterBootstrap, HeartbeatService
├── consensus/                     # Raft: node, election, log replication, facade
├── replication/                   # StorageManager, ReplicationManager, quorum,
│   └── ports/                     #   conflicts + hexagonal ports/adapters
├── faulttolerance/                # registry, failure manager, recovery
├── timesync/                      # Lamport/vector clocks, sync monitor
├── model/                         # records + enums (wire shapes)
├── util/                          # HashUtil (SHA-256 block ids, checksums)
└── web/                           # REST controllers (one per concern)
scripts/                           # run-cluster / run-node / stop-cluster (ps1 + sh)
```

---

## Porting notes (Python → Java)

These are the deliberate mechanical translations; behaviour is preserved.

- **Concurrency model.** The Python node runs one asyncio event loop with many
  coroutines. Java has real threads, so each background coroutine (election
  monitor, heartbeat sender, replication-queue worker, failure monitor,
  checkpointer) maps to a **virtual thread**, and all Raft state is guarded by a
  single `ReentrantLock`. RPC fan-out is always done **outside** the lock — the
  lock is never held across network I/O.
- **Inter-node RPC.** `httpx` → `java.net.http.HttpClient` with the same tight
  per-request timeouts (0.2 / 0.5 / 2.0 s). Blocks are hex-encoded inside JSON
  for binary safety, identical to the `/replicate` contract.
- **Wire fidelity.** All request/response bodies are built from explicit
  `Map<String,Object>` / `JsonNode` rather than bound POJOs, guaranteeing the
  JSON shapes match the original exactly (so the existing UI keeps working).
- **Enums.** Raft states serialise as lowercase `follower` / `candidate` /
  `leader`; node health as `healthy` / `suspected` / `failed` / `recovering`.
- **Checkpoints.** The original pickled checkpoints to disk; this port writes
  **JSON** checkpoints instead (portable, no Java-serialization pitfalls). The
  on-disk policy is the same: one timestamped file per checkpoint, keep the
  newest 3, every 30 s.
- **Storage durability.** Writes go through a WAL and are `fsync`-ed
  (`FileOutputStream.getFD().sync()`), with a post-write size check and SHA-256
  checksums — matching `storage.py`.

---

## Configuration

`src/main/resources/application.yml`:

- `server.port` is driven by the `PORT` env var (default `8001`).
- Multipart upload limit is 512 MB (block splitting handles large files).

Cluster membership is fixed for a local 5-node setup in `ClusterConfig`
(`node1`→`http://127.0.0.1:8001` … `node5`→`8005`), matching `shared/config.py`.
