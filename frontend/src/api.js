// Cluster API client. The dashboard runs in the browser, so it addresses each
// node at its host-mapped port. Override via VITE_DFS_NODES at build time.

const DEFAULT_NODES = [
  'http://localhost:8001',
  'http://localhost:8002',
  'http://localhost:8003',
  'http://localhost:8004',
  'http://localhost:8005',
];

export const NODE_URLS = import.meta.env.VITE_DFS_NODES
  ? import.meta.env.VITE_DFS_NODES.split(',').map((s) => s.trim()).filter(Boolean)
  : DEFAULT_NODES;

const POLL_TIMEOUT_MS = 2500;

async function getJson(base, path, timeout = POLL_TIMEOUT_MS) {
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeout);
    const res = await fetch(base + path, { signal: ctrl.signal });
    clearTimeout(t);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

// Poll every node once. Returns one record per node.
export async function pollCluster() {
  return Promise.all(
    NODE_URLS.map(async (url) => {
      const [health, metrics] = await Promise.all([
        getJson(url, '/health'),
        getJson(url, '/api/metrics'),
      ]);
      const reachable = !!(health || metrics);
      const nodeId = health?.node || metrics?.consensus?.leader || url;
      return { url, reachable, nodeId, health: health || {}, metrics: metrics || {} };
    })
  );
}

// The true leader reports its own state as 'leader'; otherwise take the majority view.
export function detectLeader(nodes) {
  for (const n of nodes) {
    if (n.reachable && n.metrics?.consensus?.state === 'leader') return n.nodeId;
  }
  const votes = {};
  for (const n of nodes) {
    const l = n.metrics?.consensus?.leader;
    if (l) votes[l] = (votes[l] || 0) + 1;
  }
  const entries = Object.entries(votes).sort((a, b) => b[1] - a[1]);
  return entries.length ? entries[0][0] : null;
}

export function idToUrl(nodes) {
  const m = {};
  for (const n of nodes) if (n.reachable) m[n.nodeId] = n.url;
  return m;
}

export function leaderUrl(nodes, leaderId) {
  const m = idToUrl(nodes);
  if (m[leaderId]) return m[leaderId];
  const r = nodes.find((n) => n.reachable);
  return r ? r.url : null;
}

export function getStatus(url) {
  return getJson(url, '/status', 3000);
}

export async function uploadFile(nodes, leaderId, file) {
  const url = leaderUrl(nodes, leaderId);
  if (!url) throw new Error('No leader available to accept the write');
  const form = new FormData();
  form.append('file', file, file.name);

  let res = await fetch(`${url}/files/${encodeURIComponent(file.name)}`, { method: 'POST', body: form });
  let data = await res.json().catch(() => ({}));

  // If we hit a follower, retry against the reported leader (mapped to its host URL).
  if (data.status === 'error' && data.leader) {
    const hostUrl = idToUrl(nodes)[data.leader];
    if (hostUrl) {
      res = await fetch(`${hostUrl}/files/${encodeURIComponent(file.name)}`, { method: 'POST', body: form });
      data = await res.json().catch(() => ({}));
    }
  }
  return data;
}

// Reads are linearizable: any node forwards to the leader, so reading from any
// reachable node returns committed state.
export async function downloadFile(nodes, sourceId, filename) {
  const url = idToUrl(nodes)[sourceId];
  if (!url) return { ok: false, error: 'Node unreachable' };
  const res = await fetch(`${url}/files/${encodeURIComponent(filename)}`);
  const ct = res.headers.get('Content-Type') || '';
  if (res.ok && !ct.includes('application/json')) {
    return { ok: true, blob: await res.blob() };
  }
  const data = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
  return { ok: false, error: data.error || `HTTP ${res.status}` };
}

export async function deleteFile(nodes, leaderId, filename) {
  const url = leaderUrl(nodes, leaderId);
  if (!url) throw new Error('No leader available to accept the delete');
  const res = await fetch(`${url}/files/${encodeURIComponent(filename)}`, { method: 'DELETE' });
  const data = await res.json().catch(() => ({}));
  return { status: res.status, data };
}
