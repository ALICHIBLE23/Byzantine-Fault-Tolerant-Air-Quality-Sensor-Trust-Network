"""Concurrency-control benchmark (E8): the four shared-reputation strategies against real Redis.

Simulated aggregators (workers) apply evidence to a pool of shared reputation records at the same
time. Fewer records means more writers per record, i.e. more contention. For every run it measures
throughput, update latency, optimistic retries, lock waiting and LOST UPDATES (updates that reported
success but whose effect was overwritten). It uses exactly the code the aggregators run
(services/aggregator/reputation.py).

Run from the repo root with the stack up (it uses Redis on localhost and cleans up after itself):
    python -m experiments.concurrency_bench
    python -m experiments.concurrency_bench --workers 8 --updates 150 --think-ms 2 --keys 1,4,16,64,256
Results: results/e8_concurrency.csv and results/e8_concurrency.png
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import random
import statistics
import time
import uuid
from pathlib import Path

import redis.asyncio as aioredis

from services.aggregator.reputation import STRATEGIES, SharedReputation, rep_key


async def run_case(r, strategy: str, workers: int, keys: int, updates: int, think_ms: float) -> dict:
    tag = uuid.uuid4().hex[:8]
    store = SharedReputation(r, strategy, think_ms=think_ms)
    latencies: list[float] = []
    retries = 0
    waits: list[float] = []

    async def worker(w: int) -> None:
        nonlocal retries
        rng = random.Random(w)
        for i in range(updates):
            sid = f"bench-{tag}-{rng.randrange(keys)}"
            started = time.perf_counter()
            _, _, retry, wait = await store.apply(sid, rng.random(), f"w{w}", i, prior=0.5)
            latencies.append(time.perf_counter() - started)
            retries += retry
            waits.append(wait)

    started = time.perf_counter()
    await asyncio.gather(*(worker(w) for w in range(workers)))
    elapsed = time.perf_counter() - started

    names = [rep_key(f"bench-{tag}-{k}") for k in range(keys)]
    counts = [await r.hget(n, "n") for n in names]
    applied = sum(int(c) for c in counts if c is not None)
    await r.delete(*names)
    expected = workers * updates
    latencies.sort()
    return {
        "strategy": strategy, "workers": workers, "keys": keys, "think_ms": think_ms,
        "updates": expected, "throughput": expected / elapsed,
        "p50_ms": 1000 * latencies[len(latencies) // 2], "p95_ms": 1000 * latencies[int(len(latencies) * 0.95)],
        "retries": retries, "lock_wait_ms": 1000 * statistics.fmean(waits),
        "lost": expected - applied, "lost_pct": 100 * (expected - applied) / expected,
    }


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workers", type=int, default=8, help="concurrent writers (simulated aggregators)")
    ap.add_argument("--updates", type=int, default=150, help="updates per worker")
    ap.add_argument("--think-ms", type=float, default=2.0, help="delay between read and write")
    ap.add_argument("--keys", default="1,4,16,64,256", help="shared records (fewer = more contention)")
    ap.add_argument("--strategies", default=",".join(STRATEGIES))
    args = ap.parse_args()
    r = aioredis.Redis(host="localhost", port=int(os.getenv("REDIS_PORT", "6379")), max_connections=64)
    await r.ping()

    rows = []
    print(f"\n{args.workers} writers x {args.updates} updates, {args.think_ms} ms between read and write\n")
    print(f"  {'strategy':<11}{'records':>8}{'ops/s':>9}{'p50 ms':>8}{'p95 ms':>8}{'retries':>9}"
          f"{'lock ms':>9}{'lost':>7}{'lost %':>8}")
    for keys in [int(k) for k in args.keys.split(",")]:
        for strategy in args.strategies.split(","):
            row = await run_case(r, strategy, args.workers, keys, args.updates, args.think_ms)
            rows.append(row)
            print(f"  {strategy:<11}{keys:>8}{row['throughput']:>9.0f}{row['p50_ms']:>8.2f}{row['p95_ms']:>8.2f}"
                  f"{row['retries']:>9}{row['lock_wait_ms']:>9.2f}{row['lost']:>7}{row['lost_pct']:>7.1f}%")
        print()
    await r.aclose()

    Path("results").mkdir(exist_ok=True)
    with open("results/e8_concurrency.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
        for strategy in args.strategies.split(","):
            pts = [x for x in rows if x["strategy"] == strategy]
            ks = [x["keys"] for x in pts]
            axes[0].plot(ks, [x["lost_pct"] for x in pts], marker="o", label=strategy)
            axes[1].plot(ks, [x["throughput"] for x in pts], marker="o", label=strategy)
            axes[2].plot(ks, [x["p95_ms"] for x in pts], marker="o", label=strategy)
        for ax, title, ylabel in zip(axes, ["Lost updates", "Throughput", "Update latency (p95)"],
                                     ["% of updates lost", "updates / s", "ms"]):
            ax.set_xscale("log")
            ax.set_xlabel("shared records (fewer = more contention)")
            ax.set_title(title)
            ax.set_ylabel(ylabel)
            ax.grid(alpha=0.3)
            ax.legend()
        fig.suptitle(f"Concurrency control on shared reputations ({args.workers} writers, "
                     f"{args.think_ms} ms read-to-write gap)")
        fig.tight_layout()
        fig.savefig("results/e8_concurrency.png", dpi=150)
        print("Saved results/e8_concurrency.csv and results/e8_concurrency.png\n")
    except ImportError:
        print("Saved results/e8_concurrency.csv (matplotlib missing: no plot)\n")


if __name__ == "__main__":
    asyncio.run(main())
