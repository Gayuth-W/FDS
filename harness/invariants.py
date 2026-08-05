"""The four correctness invariants the harness verifies.

Each takes observations gathered during a scenario and returns (passed, detail).
Together they back the claim: a linearizable, replicated store that loses no
acknowledged write under partitions and crashes.
"""


def single_leader(leader_samples):
    """No split-brain: within any single observation, no two distinct nodes may
    claim leadership in the same term."""
    for sample in leader_samples:
        by_term = {}
        for nid, view in sample.items():
            if view.get("state") == "leader":
                by_term.setdefault(view.get("term"), set()).add(nid)
        for term, leaders in by_term.items():
            if len(leaders) > 1:
                return False, f"term {term} had multiple leaders: {sorted(leaders)}"
    return True, f"{len(leader_samples)} samples, always <=1 leader per term"


def no_acked_write_loss(missing, mismatched):
    """Every acknowledged write is readable afterwards with identical bytes."""
    if missing or mismatched:
        return False, f"missing={missing[:5]} mismatched={mismatched[:5]}"
    return True, "all acknowledged writes present and byte-identical"


def quorum_safety(acked_during_quorum_loss):
    """While a majority is unavailable, the cluster must not acknowledge writes."""
    if acked_during_quorum_loss > 0:
        return False, f"{acked_during_quorum_loss} writes acked without quorum"
    return True, "no writes acknowledged without a majority"


def read_your_writes(ryw_failures):
    """Immediately after a write is acked, a read (from any node) returns it."""
    if ryw_failures:
        return False, f"{len(ryw_failures)} stale/failed reads, e.g. {ryw_failures[0]}"
    return True, "every acknowledged write was immediately readable"
