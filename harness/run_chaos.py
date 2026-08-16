#!/usr/bin/env python3
"""Chaos harness: inject partitions / crashes / stalls (and optional clock skew)
into the running 5-node cluster, keep a workload going throughout, then verify
four correctness invariants.

Usage:
    python run_chaos.py --scenarios 8
    python run_chaos.py --scenarios 12 --seed 7
    ENABLE_FAKETIME=1 docker compose up --build   # (then)
    python run_chaos.py --scenarios 10 --clock-skew

Exits non-zero if any invariant is violated, so it is CI-friendly.
"""
import argparse
import random
import sys
import time

from cluster import Cluster
from faults import FaultInjector
from workload import Workload
import invariants

N = 5
QUORUM = N // 2 + 1  # 3


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def restore_all(fx, cluster):
    """Best-effort return to a clean 5-node state between scenarios."""
    for n in cluster.nodes:
        nid = n["id"]
        fx.unpause(nid)
        fx.reset_clock(nid)
        fx.heal_partition(nid)
        fx.restart(nid)
    cluster.wait_for_alive(N, timeout=40)
    cluster.wait_for_leader(timeout=30)


def pick(ids, k, avoid=None):
    pool = [i for i in ids if i != avoid] if avoid else list(ids)
    random.shuffle(pool)
    return pool[:k]


def build_scenarios(cluster, fx):
    """Each scenario returns (name, inject(), heal(), quorum_lost)."""
    def single_crash():
        leader = cluster.leader()
        victim = pick([n["id"] for n in cluster.nodes], 1)[0]
        target = leader or victim  # prefer crashing the leader to exercise election
        return (f"crash leader/1 node ({target})",
                lambda: fx.crash(target), lambda: fx.restart(target), False)

    def double_crash():
        victims = pick([n["id"] for n in cluster.nodes], 2)
        return (f"crash 2 nodes ({','.join(victims)})",
                lambda: [fx.crash(v) for v in victims],
                lambda: [fx.restart(v) for v in victims], False)

    def partition_minority():
        victims = pick([n["id"] for n in cluster.nodes], 2)
        return (f"partition 2 nodes ({','.join(victims)})",
                lambda: [fx.partition(v) for v in victims],
                lambda: [fx.heal_partition(v) for v in victims], False)

    def slow_node():
        victim = pick([n["id"] for n in cluster.nodes], 1)[0]
        return (f"stall 1 node ({victim})",
                lambda: fx.pause(victim), lambda: fx.unpause(victim), False)

    def quorum_loss():
        victims = pick([n["id"] for n in cluster.nodes], 3)
        return (f"quorum loss: 3 nodes down ({','.join(victims)})",
                lambda: [fx.crash(v) for v in victims],
                lambda: [fx.restart(v) for v in victims], True)

    return [single_crash, double_crash, partition_minority, slow_node, quorum_loss]


def clock_skew_scenario(cluster, fx):
    victims = pick([n["id"] for n in cluster.nodes], 2)
    return (f"clock skew +45s on 2 nodes ({','.join(victims)})",
            lambda: [fx.skew_clock(v, "+45s") for v in victims],
            lambda: [fx.reset_clock(v) for v in victims], False)


def run_scenario(name, inject, heal, quorum_lost, cluster, fx, wl, leader_samples):
    log(f"scenario: {name}")
    leader_samples.append(cluster.leaders_snapshot())

    before_acked = len(wl.acked)
    inject()
    time.sleep(1.0)                       # let the fault take effect
    leader_samples.append(cluster.leaders_snapshot())

    # Keep the workload running during the fault window.
    acked, refused = wl.run_for(4.0, verify_read=True)
    leader_samples.append(cluster.leaders_snapshot())

    acked_during = len(wl.acked) - before_acked

    heal()
    ok = cluster.wait_for_alive(N, timeout=40) and bool(cluster.wait_for_leader(timeout=30))
    leader_samples.append(cluster.leaders_snapshot())
    time.sleep(1.0)

    detail = f"acked={acked} refused={refused} restabilized={ok}"
    if quorum_lost:
        detail += f" acked_during_quorum_loss={acked_during}"
    log(f"  -> {detail}")
    return acked_during if quorum_lost else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", type=int, default=8)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--network", default=None, help="override docker network name")
    ap.add_argument("--clock-skew", action="store_true",
                    help="include clock-skew scenarios (needs ENABLE_FAKETIME=1)")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    cluster = Cluster()
    fx = FaultInjector(cluster, network=args.network)
    wl = Workload(cluster, prefix="chaos")

    log(f"docker network: {fx.network}")
    log("waiting for a healthy cluster...")
    if not cluster.wait_for_alive(N, timeout=60) or not cluster.wait_for_leader(timeout=40):
        log("ERROR: cluster not healthy. Start it with `docker compose up --build` first.")
        return 2

    if args.clock_skew and not fx.faketime_enabled("node1"):
        log("WARNING: --clock-skew set but libfaketime is not active "
            "(start with ENABLE_FAKETIME=1 docker compose up). Skipping clock-skew.")
        args.clock_skew = False

    # Baseline sanity: a few clean writes with read-your-writes checks.
    log("baseline workload...")
    wl.run_for(3.0, verify_read=True)

    scenario_builders = build_scenarios(cluster, fx)
    leader_samples = []
    acked_during_quorum_loss = 0
    results = []

    for i in range(args.scenarios):
        if args.clock_skew and i % 4 == 3:
            name, inject, heal, ql = clock_skew_scenario(cluster, fx)
        else:
            name, inject, heal, ql = random.choice(scenario_builders)()
        try:
            acked_during_quorum_loss += run_scenario(
                name, inject, heal, ql, cluster, fx, wl, leader_samples)
            results.append((name, "ran"))
        except Exception as e:  # keep going; a scenario crashing shouldn't abort the suite
            log(f"  scenario error: {e}")
            results.append((name, f"error: {e}"))
            restore_all(fx, cluster)

    log("restoring cluster and settling before final verification...")
    restore_all(fx, cluster)
    time.sleep(3.0)

    log("verifying no acknowledged write was lost...")
    missing, mismatched = wl.verify_all_acked()

    checks = [
        ("No split-brain (<=1 leader/term)", invariants.single_leader(leader_samples)),
        ("No acknowledged-write loss", invariants.no_acked_write_loss(missing, mismatched)),
        ("Quorum safety (no ack without majority)", invariants.quorum_safety(acked_during_quorum_loss)),
        ("Read-your-writes (linearizable reads)", invariants.read_your_writes(wl.ryw_failures)),
    ]

    print("\n" + "=" * 68)
    print(f" CHAOS REPORT  ({args.scenarios} scenarios, seed={args.seed})")
    print("=" * 68)
    print(f" acknowledged writes : {len(wl.acked)}")
    print(f" writes refused      : {wl.refused}  (expected during quorum loss)")
    print(f" read-your-writes    : {wl.ryw_checked} checks")
    print(f" leader samples      : {len(leader_samples)}")
    print("-" * 68)
    all_ok = True
    for label, (passed, detail) in checks:
        mark = "PASS" if passed else "FAIL"
        all_ok = all_ok and passed
        print(f" [{mark}] {label}")
        print(f"        {detail}")
    print("=" * 68)
    print(" RESULT:", "ALL INVARIANTS HELD" if all_ok else "INVARIANT VIOLATION(S) DETECTED")
    print("=" * 68 + "\n")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
