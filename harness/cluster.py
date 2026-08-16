"""Thin client over the 5-node DFS cluster used by the chaos harness and load test.

The harness runs on the host, so it addresses each node at its host-mapped port
(http://localhost:8001..8005) and refers to containers by name for fault
injection (docker stop/pause/network).
"""
import time
import requests

NODES = [
    {"id": "node1", "url": "http://localhost:8001", "container": "dfs-node1"},
    {"id": "node2", "url": "http://localhost:8002", "container": "dfs-node2"},
    {"id": "node3", "url": "http://localhost:8003", "container": "dfs-node3"},
    {"id": "node4", "url": "http://localhost:8004", "container": "dfs-node4"},
    {"id": "node5", "url": "http://localhost:8005", "container": "dfs-node5"},
]


def _is_json(resp):
    return "application/json" in resp.headers.get("Content-Type", "")


class Cluster:
    def __init__(self, nodes=None):
        self.nodes = nodes or NODES

    def urls(self):
        return {n["id"]: n["url"] for n in self.nodes}

    def container(self, nid):
        return next(n for n in self.nodes if n["id"] == nid)["container"]

    def ping(self, url, timeout=1.0):
        try:
            return requests.get(url + "/health", timeout=timeout).ok
        except requests.RequestException:
            return False

    def alive_ids(self, timeout=1.0):
        return [n["id"] for n in self.nodes if self.ping(n["url"], timeout)]

    def metrics(self, url, timeout=1.5):
        try:
            r = requests.get(url + "/api/metrics", timeout=timeout)
            return r.json() if r.ok else None
        except requests.RequestException:
            return None

    def leader(self, timeout=1.5):
        """Return the current leader id. A node reporting state=='leader' is
        authoritative; otherwise fall back to the majority-reported leader."""
        views = {}
        for n in self.nodes:
            m = self.metrics(n["url"], timeout)
            if not m:
                continue
            c = m.get("consensus", {})
            if c.get("state") == "leader":
                return n["id"]
            l = c.get("leader")
            if l:
                views[l] = views.get(l, 0) + 1
        if not views:
            return None
        return max(views.items(), key=lambda kv: kv[1])[0]

    def leader_url(self, timeout=1.5):
        lid = self.leader(timeout)
        return self.urls().get(lid) if lid else None

    def leaders_snapshot(self, timeout=1.0):
        """Every node's (state, term, leader-view) in one sample. Used for the
        split-brain check."""
        snap = {}
        for n in self.nodes:
            m = self.metrics(n["url"], timeout)
            if not m:
                continue
            c = m.get("consensus", {})
            snap[n["id"]] = {"state": c.get("state"), "term": c.get("term"), "leader": c.get("leader")}
        return snap

    def wait_for_leader(self, timeout=30.0, poll=0.5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            lid = self.leader()
            if lid:
                return lid
            time.sleep(poll)
        return None

    def wait_for_alive(self, count, timeout=40.0, poll=0.5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(self.alive_ids()) >= count:
                return True
            time.sleep(poll)
        return False

    def upload(self, filename, content, timeout=6.0):
        """Write to the leader; follow a 'not leader' hint once. Returns
        (ok, detail)."""
        url = self.leader_url()
        if not url:
            return False, "no leader"
        try:
            r = requests.post(f"{url}/files/{filename}",
                              files={"file": (filename, content)}, timeout=timeout)
            data = r.json() if _is_json(r) else {}
            if data.get("status") == "success":
                return True, data
            if data.get("status") == "error" and data.get("leader"):
                lurl = self.urls().get(data["leader"])
                if lurl:
                    r = requests.post(f"{lurl}/files/{filename}",
                                      files={"file": (filename, content)}, timeout=timeout)
                    data = r.json() if _is_json(r) else {}
                    return data.get("status") == "success", data
            return False, data or r.text
        except requests.RequestException as e:
            return False, str(e)

    def download(self, filename, from_url=None, timeout=6.0):
        """Read from a specific node (or the leader). Returns (ok, bytes, err)."""
        url = from_url or self.leader_url()
        if not url:
            return False, None, "no node"
        try:
            r = requests.get(f"{url}/files/{filename}", timeout=timeout)
            if r.ok and not _is_json(r):
                return True, r.content, None
            try:
                err = r.json().get("error", f"HTTP {r.status_code}")
            except ValueError:
                err = f"HTTP {r.status_code}"
            return False, None, err
        except requests.RequestException as e:
            return False, None, str(e)

    def delete(self, filename, timeout=6.0):
        url = self.leader_url()
        if not url:
            return False, "no leader"
        try:
            r = requests.delete(f"{url}/files/{filename}", timeout=timeout)
            data = r.json() if _is_json(r) else {}
            return r.status_code == 200, data
        except requests.RequestException as e:
            return False, str(e)
