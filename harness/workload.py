"""Workload driver that records acknowledged writes and checks read-your-writes.

The key idea behind the whole harness: we only ever assert on writes the cluster
positively acknowledged. If the cluster said "committed", that write must survive
every fault and be readable afterwards. Writes that were refused (e.g. during
quorum loss) are expected to be absent and are never asserted on.
"""
import hashlib
import os
import random
import time


def _sha(b):
    return hashlib.sha256(b).hexdigest()


class Workload:
    def __init__(self, cluster, prefix="chaos"):
        self.cluster = cluster
        self.prefix = prefix
        self.acked = {}            # filename -> sha256 of acknowledged content
        self.refused = 0           # writes the cluster declined (no quorum / no leader)
        self.ryw_checked = 0       # read-your-writes checks performed
        self.ryw_failures = []     # (filename, reason) for stale/failed immediate reads
        self._seq = 0

    def _new_payload(self, size=2048):
        self._seq += 1
        header = f"{self.prefix}-{self._seq}-{time.time_ns()}\n".encode()
        return header + os.urandom(max(0, size - len(header)))

    def write_once(self, verify_read=True):
        """Attempt one write. If acked, optionally read it straight back from a
        RANDOM reachable node to assert read-your-writes."""
        fname = f"{self.prefix}_{self._seq + 1}.bin"
        content = self._new_payload()
        ok, _ = self.cluster.upload(fname, content)
        if not ok:
            self.refused += 1
            return False
        self.acked[fname] = _sha(content)

        if verify_read:
            alive = self.cluster.alive_ids()
            if alive:
                nid = random.choice(alive)
                url = self.cluster.urls()[nid]
                rok, data, err = self.cluster.download(fname, from_url=url)
                self.ryw_checked += 1
                if not rok:
                    self.ryw_failures.append((fname, f"read from {nid}: {err}"))
                elif _sha(data) != self.acked[fname]:
                    self.ryw_failures.append((fname, f"stale content from {nid}"))
        return True

    def run_for(self, seconds, verify_read=True, gap=0.15):
        """Drive writes for a fixed duration. Returns (acked, refused)."""
        acked = refused = 0
        deadline = time.time() + seconds
        while time.time() < deadline:
            if self.write_once(verify_read=verify_read):
                acked += 1
            else:
                refused += 1
            time.sleep(gap)
        return acked, refused

    def verify_all_acked(self):
        """After the cluster heals, every acknowledged write must be readable
        from the leader with byte-identical content. Returns (missing, mismatched)."""
        missing, mismatched = [], []
        lurl = self.cluster.leader_url()
        for fname, digest in self.acked.items():
            ok, data, _ = self.cluster.download(fname, from_url=lurl)
            if not ok:
                missing.append(fname)
            elif _sha(data) != digest:
                mismatched.append(fname)
        return missing, mismatched
