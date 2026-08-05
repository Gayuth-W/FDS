"""Fault injection via the Docker CLI: crash, pause, network partition, clock skew.

All operations target containers by name (dfs-node1..dfs-node5). Requires the
cluster to be running under docker compose and the docker CLI to be on PATH.
"""
import subprocess


def _run(args):
    return subprocess.run(args, capture_output=True, text=True)


def detect_network(hint="dfsnet"):
    """Find the compose-created network (project prefix varies, e.g. fds_dfsnet)."""
    res = _run(["docker", "network", "ls", "--format", "{{.Name}}"])
    for name in res.stdout.split():
        if hint in name:
            return name
    return hint


class FaultInjector:
    def __init__(self, cluster, network=None):
        self.cluster = cluster
        self.network = network or detect_network()

    def _c(self, nid):
        return self.cluster.container(nid)

    # --- crash / restart (process + node gone) ---
    def crash(self, nid):
        _run(["docker", "stop", "-t", "1", self._c(nid)])

    def restart(self, nid):
        _run(["docker", "start", self._c(nid)])

    # --- pause / unpause (frozen process: looks like a very slow/stalled node) ---
    def pause(self, nid):
        _run(["docker", "pause", self._c(nid)])

    def unpause(self, nid):
        _run(["docker", "unpause", self._c(nid)])

    # --- network partition (node up but unreachable by peers) ---
    def partition(self, nid):
        _run(["docker", "network", "disconnect", self.network, self._c(nid)])

    def heal_partition(self, nid):
        _run(["docker", "network", "connect", self.network, self._c(nid)])

    # --- clock skew (requires the cluster started with ENABLE_FAKETIME=1) ---
    def skew_clock(self, nid, offset="+30s"):
        _run(["docker", "exec", self._c(nid), "sh", "-c",
              f'echo "{offset}" > /etc/faketimerc'])

    def reset_clock(self, nid):
        _run(["docker", "exec", self._c(nid), "sh", "-c",
              'echo "+0" > /etc/faketimerc'])

    def faketime_enabled(self, nid):
        """True if libfaketime is actually preloaded in the target container."""
        res = _run(["docker", "exec", self._c(nid), "sh", "-c",
                    'echo "$LD_PRELOAD"'])
        return "faketime" in res.stdout.lower()
