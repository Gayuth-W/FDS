import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  NODE_URLS,
  pollCluster,
  detectLeader,
  idToUrl,
  getStatus,
  leaderUrl,
  uploadFile,
  downloadFile,
  deleteFile,
} from './api.js';

function Metric({ label, value, accent }) {
  return (
    <div className="metric">
      <div className="metric-value" style={accent ? { color: accent } : undefined}>{value}</div>
      <div className="metric-label">{label}</div>
    </div>
  );
}

function StateBadge({ state, isLeader }) {
  const cls = isLeader ? 'badge leader' : `badge ${state}`;
  return <span className={cls}>{isLeader ? '\u{1F451} LEADER' : (state || '?').toUpperCase()}</span>;
}

function NodeCard({ node, leaderId }) {
  if (!node.reachable) {
    return (
      <div className="node-card down">
        <div className="node-id">{node.nodeId}</div>
        <span className="badge down">UNREACHABLE</span>
      </div>
    );
  }
  const c = node.metrics.consensus || {};
  const r = node.metrics.replication || {};
  const isLeader = node.nodeId === leaderId;
  return (
    <div className={`node-card${isLeader ? ' is-leader' : ''}`}>
      <div className="node-id">{node.nodeId}</div>
      <StateBadge state={c.state} isLeader={isLeader} />
      <div className="node-term">term {c.term ?? '?'}</div>
      <div className="node-stats">
        <div><span>{r.files_stored ?? 0}</span> blocks</div>
        <div><span>{r.peer_count ?? 0}</span> peers</div>
      </div>
    </div>
  );
}

const TABS = ['Consensus', 'Replication', 'Fault Tolerance', 'Files'];

export default function App() {
  const [nodes, setNodes] = useState([]);
  const [leaderStatus, setLeaderStatus] = useState(null);
  const [loaded, setLoaded] = useState(false);
  const [auto, setAuto] = useState(true);
  const [interval, setIntervalSec] = useState(3);
  const [tab, setTab] = useState('Consensus');
  const timer = useRef(null);

  const refresh = useCallback(async () => {
    const ns = await pollCluster();
    const leader = detectLeader(ns);
    let status = null;
    const lUrl = leaderUrl(ns, leader);
    if (lUrl) status = await getStatus(lUrl);
    setNodes(ns);
    setLeaderStatus(status);
    setLoaded(true);
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    if (auto) {
      timer.current = setInterval(refresh, interval * 1000);
      return () => clearInterval(timer.current);
    }
  }, [auto, interval, refresh]);

  const reachable = nodes.filter((n) => n.reachable);
  const leaderId = detectLeader(nodes);
  const summaryMetrics =
    (reachable.find((n) => n.nodeId === leaderId) || reachable[0])?.metrics || {};
  const ft = summaryMetrics.fault_tolerance || {};
  const totalBlocks = reachable.reduce(
    (s, n) => s + (n.metrics.replication?.files_stored || 0), 0);
  const maxLamport = reachable.reduce(
    (m, n) => Math.max(m, n.metrics.time_sync?.lamport_counter || 0), 0);

  return (
    <div className="app">
      <header>
        <div>
          <h1>Distributed Store &mdash; Cluster Console</h1>
          <p className="subtitle">5-node Raft &middot; linearizable reads &middot; consensus-committed deletes</p>
        </div>
        <div className="controls">
          <label className="toggle">
            <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.target.checked)} />
            Auto-refresh
          </label>
          <select value={interval} onChange={(e) => setIntervalSec(Number(e.target.value))} disabled={!auto}>
            {[1, 2, 3, 5, 10].map((s) => <option key={s} value={s}>{s}s</option>)}
          </select>
          <button onClick={refresh}>Refresh</button>
        </div>
      </header>

      {!loaded && <div className="notice">Connecting to cluster&hellip;</div>}

      {loaded && reachable.length === 0 && (
        <div className="notice error">
          No nodes reachable. Start the cluster (<code>docker compose up</code>) and confirm the
          nodes are listening at {NODE_URLS.join(', ')}.
        </div>
      )}

      {reachable.length > 0 && (
        <>
          <section className="summary">
            <Metric label="Leader" value={leaderId || '\u2014'} accent="#4ade80" />
            <Metric label="Reachable" value={`${reachable.length} / ${nodes.length}`} />
            <Metric label="Suspected" value={ft.suspected_counts ?? 0} accent="#fbbf24" />
            <Metric label="Failed" value={ft.failed_counts ?? 0} accent="#f87171" />
            <Metric label="Blocks (total)" value={totalBlocks} />
            <Metric label="Max Lamport" value={maxLamport} />
          </section>

          <section className="node-grid">
            {nodes.map((n) => <NodeCard key={n.url} node={n} leaderId={leaderId} />)}
          </section>

          <nav className="tabs">
            {TABS.map((t) => (
              <button key={t} className={t === tab ? 'active' : ''} onClick={() => setTab(t)}>{t}</button>
            ))}
          </nav>

          <section className="panel">
            {tab === 'Consensus' && <ConsensusTab nodes={nodes} />}
            {tab === 'Replication' && <ReplicationTab nodes={nodes} leaderStatus={leaderStatus} />}
            {tab === 'Fault Tolerance' && <FaultTab ft={ft} leaderStatus={leaderStatus} />}
            {tab === 'Files' && <FilesTab nodes={nodes} reachable={reachable} leaderId={leaderId} />}
          </section>
        </>
      )}

      <footer>Reads are served by the leader (forwarded automatically); writes and deletes are Raft-committed.</footer>
    </div>
  );
}

function ConsensusTab({ nodes }) {
  return (
    <table>
      <thead>
        <tr><th>Node</th><th>Reachable</th><th>State</th><th>Term</th><th>Leader (its view)</th></tr>
      </thead>
      <tbody>
        {nodes.map((n) => {
          const c = n.metrics.consensus || {};
          return (
            <tr key={n.url}>
              <td>{n.nodeId}</td>
              <td>{n.reachable ? 'yes' : 'no'}</td>
              <td>{c.state || '\u2014'}</td>
              <td>{c.term ?? '\u2014'}</td>
              <td>{c.leader || '\u2014'}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function ReplicationTab({ nodes, leaderStatus }) {
  const rep = leaderStatus?.replication || {};
  return (
    <>
      <div className="stat-row">
        <Metric label="Total replications" value={rep.total_replications ?? '\u2014'} />
        <Metric label="Successful" value={rep.successful ?? '\u2014'} accent="#4ade80" />
        <Metric label="Failed" value={rep.failed ?? '\u2014'} accent="#f87171" />
        <Metric label="Retries" value={rep.retries ?? '\u2014'} />
      </div>
      <table>
        <thead><tr><th>Node</th><th>Blocks stored</th><th>Peers</th></tr></thead>
        <tbody>
          {nodes.filter((n) => n.reachable).map((n) => {
            const r = n.metrics.replication || {};
            return (
              <tr key={n.url}>
                <td>{n.nodeId}</td>
                <td>{r.files_stored ?? 0}</td>
                <td>{(r.peers || []).join(', ') || '\u2014'}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </>
  );
}

function FaultTab({ ft, leaderStatus }) {
  const details = ft.node_status_details || {};
  const system = leaderStatus?.system;
  return (
    <>
      {system && (
        <div className="stat-row">
          <Metric label="System status" value={system.status || '\u2014'} />
          <Metric label="Live nodes" value={(system.live_nodes || []).length} />
          <Metric label="Recovering" value={String(system.recovering ?? false)} />
        </div>
      )}
      <table>
        <thead><tr><th>Node</th><th>Health</th></tr></thead>
        <tbody>
          {Object.entries(details).map(([k, v]) => (
            <tr key={k}><td>{k}</td><td className={`health ${v}`}>{v}</td></tr>
          ))}
          {Object.keys(details).length === 0 && (
            <tr><td colSpan={2} className="muted">No status details reported yet.</td></tr>
          )}
        </tbody>
      </table>
      {system?.failed_nodes?.length > 0 && (
        <p className="muted">Failed: {system.failed_nodes.join(', ')}</p>
      )}
    </>
  );
}

function FilesTab({ nodes, reachable, leaderId }) {
  const [file, setFile] = useState(null);
  const [dlName, setDlName] = useState('');
  const [dlSource, setDlSource] = useState(reachable[0]?.nodeId || '');
  const [delName, setDelName] = useState('');
  const [msg, setMsg] = useState(null);

  const say = (text, kind = 'info') => setMsg({ text, kind });

  const onUpload = async () => {
    if (!file) return;
    try {
      const data = await uploadFile(nodes, leaderId, file);
      if (data.status === 'success') {
        say(`Stored '${data.filename}' \u2014 ${data.total_size} bytes in ${data.blocks} block(s); ${data.consensus}.`, 'ok');
      } else {
        say(`Server response: ${JSON.stringify(data)}`, 'warn');
      }
    } catch (e) { say(String(e.message || e), 'error'); }
  };

  const onDownload = async () => {
    if (!dlName) return;
    try {
      const res = await downloadFile(nodes, dlSource, dlName);
      if (res.ok) {
        const url = URL.createObjectURL(res.blob);
        const a = document.createElement('a');
        a.href = url; a.download = dlName; a.click();
        URL.revokeObjectURL(url);
        say(`Downloaded '${dlName}' (${res.blob.size} bytes).`, 'ok');
      } else {
        say(`Not available: ${res.error}`, 'warn');
      }
    } catch (e) { say(String(e.message || e), 'error'); }
  };

  const onDelete = async () => {
    if (!delName) return;
    try {
      const { status, data } = await deleteFile(nodes, leaderId, delName);
      if (status === 200) say(data.message || 'Deleted.', 'ok');
      else say(`HTTP ${status}: ${JSON.stringify(data)}`, 'warn');
    } catch (e) { say(String(e.message || e), 'error'); }
  };

  return (
    <div className="files">
      <p className="muted">Writes and deletes go to the leader; reads are served consistently from any node.</p>

      <div className="file-block">
        <h3>Upload</h3>
        <input type="file" onChange={(e) => setFile(e.target.files[0] || null)} />
        <button onClick={onUpload} disabled={!file}>Upload to cluster</button>
      </div>

      <div className="file-block">
        <h3>Download</h3>
        <input placeholder="filename" value={dlName} onChange={(e) => setDlName(e.target.value)} />
        <select value={dlSource} onChange={(e) => setDlSource(e.target.value)}>
          {reachable.map((n) => <option key={n.nodeId} value={n.nodeId}>{n.nodeId}</option>)}
        </select>
        <button onClick={onDownload} disabled={!dlName}>Fetch</button>
      </div>

      <div className="file-block">
        <h3>Delete</h3>
        <input placeholder="filename" value={delName} onChange={(e) => setDelName(e.target.value)} />
        <button onClick={onDelete} disabled={!delName}>Delete from cluster</button>
      </div>

      {msg && <div className={`msg ${msg.kind}`}>{msg.text}</div>}
    </div>
  );
}
