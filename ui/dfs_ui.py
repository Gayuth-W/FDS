"""
Streamlit dashboard for the Java/Spring Boot Distributed File System.

Talks to the cluster over the same REST API as the original Python UI:
  /health, /api/metrics, /status, /files/{name}, /benchmark/*

Node base URLs are read from the DFS_NODES env var (comma-separated), so the
same app works whether you run it locally against a local cluster or inside
Docker Compose against the container network.

Run locally:
    pip install -r requirements.txt
    streamlit run dfs_ui.py
"""

import os
import time
from collections import Counter

import requests
import streamlit as st

# Configuration
DEFAULT_NODES = ",".join(f"http://localhost:{p}" for p in range(8001, 8006))
NODE_URLS = [u.strip() for u in os.getenv("DFS_NODES", DEFAULT_NODES).split(",") if u.strip()]

REQUEST_TIMEOUT = 2.0  # seconds for dashboard polls

st.set_page_config(page_title="DFS Cluster", page_icon="🗄️", layout="wide")


# HTTP helpers
def get_json(base_url: str, path: str, timeout: float = REQUEST_TIMEOUT):
    """GET {base_url}{path} and return parsed JSON, or None on any failure."""
    try:
        resp = requests.get(f"{base_url}{path}", timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException:
        return None
    return None


def poll_cluster():
    """
    Poll every node once. Returns a list of per-node dicts:
      { url, reachable, node_id, health, metrics, status }
    """
    nodes = []
    for url in NODE_URLS:
        health = get_json(url, "/health")
        metrics = get_json(url, "/api/metrics")
        reachable = health is not None or metrics is not None
        node_id = None
        if health:
            node_id = health.get("node")
        if node_id is None and metrics:
            node_id = (metrics.get("consensus") or {}).get("leader")
        nodes.append({
            "url": url,
            "reachable": reachable,
            "node_id": node_id or url,
            "health": health or {},
            "metrics": metrics or {},
        })
    return nodes


def id_to_url_map(nodes):
    return {n["node_id"]: n["url"] for n in nodes if n["reachable"]}


def detect_leader(nodes):
    """
    The true leader is the node reporting its own state as 'leader'.
    Fall back to the most commonly reported leader among reachable nodes.
    """
    for n in nodes:
        if n["reachable"]:
            state = (n["metrics"].get("consensus") or {}).get("state")
            if state == "leader":
                return n["node_id"]
    votes = Counter()
    for n in nodes:
        leader = (n["metrics"].get("consensus") or {}).get("leader")
        if leader:
            votes[leader] += 1
    return votes.most_common(1)[0][0] if votes else None


def leader_base_url(nodes, leader_id):
    mapping = id_to_url_map(nodes)
    if leader_id in mapping:
        return mapping[leader_id]
    # Fall back to any reachable node; it will redirect writes to the leader.
    for n in nodes:
        if n["reachable"]:
            return n["url"]
    return None

# Sidebar
with st.sidebar:
    st.header("⚙️ Controls")
    auto = st.checkbox("Auto-refresh", value=False)
    interval = st.slider("Interval (s)", 1, 10, 3, disabled=not auto)
    if st.button("🔄 Refresh now", use_container_width=True):
        st.rerun()
    st.divider()
    st.caption("Configured nodes")
    for u in NODE_URLS:
        st.caption(f"• {u}")
    st.caption("Set the `DFS_NODES` env var to point elsewhere.")

# Poll once per render
nodes = poll_cluster()
reachable = [n for n in nodes if n["reachable"]]
leader_id = detect_leader(nodes)

st.title("🗄️ Distributed File System — Cluster Dashboard")

if not reachable:
    st.error(
        "No nodes are reachable. Start the cluster (e.g. `docker compose up`) "
        "and confirm `DFS_NODES` matches where the nodes are listening."
    )
    st.stop()

# Cluster summary
# Prefer the leader's view for cluster-wide fault-tolerance counters.
summary_metrics = {}
for n in reachable:
    if n["node_id"] == leader_id:
        summary_metrics = n["metrics"]
        break
if not summary_metrics:
    summary_metrics = reachable[0]["metrics"]

ft = summary_metrics.get("fault_tolerance") or {}
total_blocks = sum((n["metrics"].get("replication") or {}).get("files_stored", 0) for n in reachable)
max_lamport = max(((n["metrics"].get("time_sync") or {}).get("lamport_counter", 0) for n in reachable), default=0)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Leader", leader_id or "—")
c2.metric("Reachable nodes", f"{len(reachable)} / {len(nodes)}")
c3.metric("Suspected", ft.get("suspected_counts", 0))
c4.metric("Failed", ft.get("failed_counts", 0))
c5.metric("Max Lamport", max_lamport)

st.divider()

# Per-node grid
st.subheader("Nodes")
cols = st.columns(len(nodes))
for col, n in zip(cols, nodes):
    with col:
        nid = n["node_id"]
        if not n["reachable"]:
            st.markdown(f"**{nid}**")
            st.error("unreachable")
            continue
        consensus = n["metrics"].get("consensus") or {}
        repl = n["metrics"].get("replication") or {}
        state = consensus.get("state", "?")
        is_leader = nid == leader_id
        badge = "👑 LEADER" if is_leader else state.upper()
        st.markdown(f"**{nid}**")
        (st.success if is_leader else st.info)(badge)
        st.caption(f"term {consensus.get('term', '?')}")
        st.metric("Blocks", repl.get("files_stored", 0))
        st.metric("Peers", repl.get("peer_count", 0))

st.divider()

# Detail tabs
tab_consensus, tab_repl, tab_fault, tab_files, tab_bench = st.tabs(
    ["🧭 Consensus", "📦 Replication", "🛡️ Fault Tolerance", "📁 Files", "⏱️ Benchmark"]
)

with tab_consensus:
    st.write("Each node's Raft view:")
    rows = []
    for n in nodes:
        c = n["metrics"].get("consensus") or {}
        rows.append({
            "node": n["node_id"],
            "reachable": n["reachable"],
            "state": c.get("state", "—"),
            "term": c.get("term", "—"),
            "leader (its view)": c.get("leader", "—"),
        })
    st.dataframe(rows, use_container_width=True, hide_index=True)

with tab_repl:
    st.write("Replication statistics (from each node's `/status`):")
    rows = []
    for n in reachable:
        status = get_json(n["url"], "/status") or {}
        rep = status.get("replication") or {}
        storage = status.get("storage") or {}
        rows.append({
            "node": n["node_id"],
            "blocks": storage.get("block_count", "—"),
            "total_replications": rep.get("total_replications", "—"),
            "successful": rep.get("successful", "—"),
            "failed": rep.get("failed", "—"),
            "retries": rep.get("retries", "—"),
        })
    st.dataframe(rows, use_container_width=True, hide_index=True)

with tab_fault:
    st.write("Per-node health as seen by the leader's failure detector:")
    details = ft.get("node_status_details") or {}
    if details:
        st.dataframe(
            [{"node": k, "status": v} for k, v in details.items()],
            use_container_width=True, hide_index=True,
        )
    else:
        st.caption("No status details reported yet.")
    system = None
    lead_url = leader_base_url(nodes, leader_id)
    if lead_url:
        status = get_json(lead_url, "/status") or {}
        system = status.get("system")
    if system:
        s1, s2, s3 = st.columns(3)
        s1.metric("System status", system.get("status", "—"))
        s2.metric("Live nodes", len(system.get("live_nodes", [])))
        s3.metric("Recovering", str(system.get("recovering", False)))
        st.caption(f"Live: {system.get('live_nodes', [])}")
        if system.get("failed_nodes"):
            st.caption(f"Failed: {system.get('failed_nodes')}")

with tab_files:
    st.write("Writes go to the **leader**; reads/deletes are routed for you.")
    lead_url = leader_base_url(nodes, leader_id)

    st.markdown("**Upload**")
    up = st.file_uploader("Choose a file to store in the cluster", key="uploader")
    if up is not None and st.button("Upload to cluster", type="primary"):
        if not lead_url:
            st.error("No leader available to accept the write.")
        else:
            try:
                content = up.getvalue()
                r = requests.post(
                    f"{lead_url}/files/{up.name}",
                    files={"file": (up.name, content)},
                    timeout=30,
                )
                data = r.json() if "application/json" in r.headers.get("Content-Type", "") else {}
                # If we happened to hit a non-leader, follow its redirect once.
                if data.get("status") == "error" and data.get("leader_url"):
                    r = requests.post(
                        f"{data['leader_url']}/files/{up.name}",
                        files={"file": (up.name, content)},
                        timeout=30,
                    )
                    data = r.json()
                if data.get("status") == "success":
                    st.success(
                        f"Stored '{data['filename']}' — {data['total_size']} bytes "
                        f"in {data['blocks']} block(s); consensus {data.get('consensus')}."
                    )
                else:
                    st.warning(f"Server response: {data or r.text}")
            except requests.RequestException as e:
                st.error(f"Upload failed: {e}")

    st.divider()
    st.markdown("**Download**")
    dl_name = st.text_input("Filename to download", key="dl")
    dl_source = st.selectbox(
        "Read from", [n["node_id"] for n in reachable],
        index=([n["node_id"] for n in reachable].index(leader_id)
               if leader_id in [n["node_id"] for n in reachable] else 0),
        key="dl_src",
    )
    if dl_name and st.button("Fetch file"):
        src_url = id_to_url_map(nodes).get(dl_source)
        try:
            r = requests.get(f"{src_url}/files/{dl_name}", timeout=30)
            ctype = r.headers.get("Content-Type", "")
            if r.status_code == 200 and "application/json" not in ctype:
                st.success(f"Retrieved {len(r.content)} bytes.")
                st.download_button("💾 Save file", data=r.content, file_name=dl_name)
            else:
                st.warning(f"Not available: {r.json() if 'json' in ctype else r.text}")
        except requests.RequestException as e:
            st.error(f"Download failed: {e}")

    st.divider()
    st.markdown("**Delete**")
    del_name = st.text_input("Filename to delete", key="del")
    if del_name and st.button("Delete from cluster"):
        if not lead_url:
            st.error("No leader available to accept the delete.")
        else:
            try:
                r = requests.delete(f"{lead_url}/files/{del_name}", timeout=30)
                if r.status_code == 200:
                    st.success(r.json().get("message", "Deleted."))
                else:
                    body = r.json() if "json" in r.headers.get("Content-Type", "") else r.text
                    st.warning(f"HTTP {r.status_code}: {body}")
            except requests.RequestException as e:
                st.error(f"Delete failed: {e}")

with tab_bench:
    st.write("Measure single-node write latency via `/benchmark/upload_latency`.")
    bench_node = st.selectbox("Run against", [n["node_id"] for n in reachable], key="bench_src")
    size_kb = st.slider("Payload size (KB)", 1, 1024, 64)
    if st.button("Run benchmark"):
        url = id_to_url_map(nodes).get(bench_node)
        payload = b"x" * (size_kb * 1024)
        try:
            r = requests.post(
                f"{url}/benchmark/upload_latency",
                files={"file": ("bench.bin", payload)},
                timeout=30,
            )
            if r.status_code == 200:
                d = r.json()
                st.metric("Write latency", f"{d['latency_ms']:.2f} ms", help=f"{d['size']} bytes")
            else:
                st.warning(f"HTTP {r.status_code}: {r.text}")
        except requests.RequestException as e:
            st.error(f"Benchmark failed: {e}")

    hist = get_json(id_to_url_map(nodes).get(bench_node, reachable[0]["url"]), "/api/benchmarks")
    if hist and hist.get("data"):
        st.caption("Recent samples on this node:")
        st.dataframe(hist["data"][-20:], use_container_width=True, hide_index=True)

# Auto-refresh
if auto:
    time.sleep(interval)
    st.rerun()
