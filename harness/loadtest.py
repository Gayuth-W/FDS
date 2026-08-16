#!/usr/bin/env python3
"""Closed-loop load generator: many concurrent clients writing to the cluster,
reporting throughput and latency percentiles (the numbers for the write path).

Usage:
    python loadtest.py --clients 16 --duration 20
    python loadtest.py --clients 24 --duration 30 --size 4096 --read-ratio 0.3

Reported P99 is measured client-side end-to-end (request issued -> ack), which is
the honest, reproducible figure to quote.
"""
import argparse
import os
import statistics
import threading
import time

from cluster import Cluster


def worker(cluster, stop_at, size, read_ratio, out):
    lat_w, lat_r, ok_w, err_w, ok_r, err_r = [], [], 0, 0, 0, 0
    seq = 0
    tid = threading.get_ident()
    while time.time() < stop_at:
        seq += 1
        do_read = read_ratio > 0 and (seq % max(1, int(1 / read_ratio)) == 0)
        if do_read and out["last_file"]:
            t0 = time.perf_counter()
            rok, _, _ = cluster.download(out["last_file"])
            dt = (time.perf_counter() - t0) * 1000
            lat_r.append(dt)
            ok_r += rok
            err_r += (not rok)
        else:
            fname = f"load_{tid}_{seq}.bin"
            content = os.urandom(size)
            t0 = time.perf_counter()
            wok, _ = cluster.upload(fname, content)
            dt = (time.perf_counter() - t0) * 1000
            lat_w.append(dt)
            if wok:
                ok_w += 1
                out["last_file"] = fname
            else:
                err_w += 1
    out["lat_w"] += lat_w
    out["lat_r"] += lat_r
    out["ok_w"] += ok_w
    out["err_w"] += err_w
    out["ok_r"] += ok_r
    out["err_r"] += err_r


def pct(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    k = min(len(values) - 1, int(round((p / 100.0) * (len(values) - 1))))
    return values[k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=16)
    ap.add_argument("--duration", type=int, default=20)
    ap.add_argument("--size", type=int, default=2048, help="payload bytes per write")
    ap.add_argument("--read-ratio", type=float, default=0.0, help="fraction of ops that are reads")
    args = ap.parse_args()

    cluster = Cluster()
    if not cluster.wait_for_leader(timeout=30):
        print("ERROR: no leader; start the cluster with `docker compose up --build`.")
        return 2

    shared = {"lat_w": [], "lat_r": [], "ok_w": 0, "err_w": 0, "ok_r": 0, "err_r": 0, "last_file": None}
    lock_wrap = shared  # single dict; threads append to lists (GIL-safe for our purposes)

    stop_at = time.time() + args.duration
    threads = [threading.Thread(target=worker,
                                args=(cluster, stop_at, args.size, args.read_ratio, lock_wrap))
               for _ in range(args.clients)]

    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0

    lat_w = shared["lat_w"]
    wps = shared["ok_w"] / elapsed if elapsed else 0

    print("\n" + "=" * 60)
    print(f" LOAD TEST  ({args.clients} clients, {args.duration}s, {args.size}B writes)")
    print("=" * 60)
    print(f" writes ok / failed : {shared['ok_w']} / {shared['err_w']}")
    print(f" throughput         : {wps:,.0f} writes/sec")
    if lat_w:
        print(f" write latency ms   : p50={pct(lat_w,50):.1f}  p95={pct(lat_w,95):.1f}  "
              f"p99={pct(lat_w,99):.1f}  max={max(lat_w):.1f}")
    if args.read_ratio > 0 and shared["lat_r"]:
        lr = shared["lat_r"]
        print(f" reads ok / failed  : {shared['ok_r']} / {shared['err_r']}")
        print(f" read latency ms    : p50={pct(lr,50):.1f}  p95={pct(lr,95):.1f}  p99={pct(lr,99):.1f}")
    print("=" * 60)
    print(" Quote reproducible figures only (measured client-side end-to-end).")
    print("=" * 60 + "\n")
    return 0


if __name__ == "__main__":
    main()
