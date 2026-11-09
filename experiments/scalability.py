"""Scalability experiment (E5): grow the sensor fleet and measure throughput, latency and CPU.

For each fleet size it restarts the simulated sensors with NUM_SENSORS=n, waits for the system to
settle, then reads Prometheus and `docker stats`. Takes about 2.5 minutes per size.
Run from the repo root with the stack up:
    python -m experiments.scalability                      # 200, 400, 800, 1600 sensors
    python -m experiments.scalability --sizes 400,1600,3200
Results: results/e5_scalability.csv and results/e5_scalability.png
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

PROM = f"http://localhost:{os.getenv('PROMETHEUS_PORT', '9090')}"


def prom(query: str) -> float:
    with urllib.request.urlopen(f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": query}), timeout=5) as r:
        result = json.load(r)["data"]["result"]
    return float(result[0]["value"][1]) if result else float("nan")


def cpu_by_service() -> dict[str, float]:
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.CPUPerc}}"],
                         capture_output=True, text=True, check=True).stdout
    cpu: dict[str, float] = {}
    for line in out.splitlines():
        name, perc = line.rsplit(" ", 1)
        if not name.startswith("trustmesh-"):
            continue
        service = name.removeprefix("trustmesh-").rsplit("-", 1)[0]
        cpu[service] = float(perc.rstrip("%"))
    return cpu


def compose_up_sensors(n: int | None) -> None:
    env = dict(os.environ)
    if n is not None:
        env["NUM_SENSORS"] = str(n)
    subprocess.run(["docker", "compose", "up", "-d", "sensors", "evaluator"], env=env, check=True,
                   capture_output=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sizes", default="200,400,800,1600")
    ap.add_argument("--warmup", type=float, default=60.0)
    ap.add_argument("--measure", type=float, default=60.0)
    ap.add_argument("--interval", type=float, default=2.0, help="PUBLISH_INTERVAL_S of the sensors")
    args = ap.parse_args()
    rows = []
    try:
        for n in [int(x) for x in args.sizes.split(",")]:
            print(f"\n{n} sensors: restarting the fleet, warming up {args.warmup:.0f} s, measuring {args.measure:.0f} s")
            compose_up_sensors(n)
            time.sleep(args.warmup + args.measure)
            cpu = cpu_by_service()
            aggs = [v for k, v in cpu.items() if k.startswith("aggregator")]
            row = {
                "sensors": n,
                "offered_per_s": n / args.interval,
                "published_per_s": prom("sum(rate(tm_sensor_published_total[1m]))"),
                "ingested_per_s": prom("sum(max by (region) (rate(tm_agg_readings_total[1m])))"),
                "window_latency_p95_s": prom(
                    "histogram_quantile(0.95, sum by (le) (rate(tm_agg_window_latency_seconds_bucket[1m])))"),
                "trust_compute_p95_ms": 1000 * prom(
                    "histogram_quantile(0.95, sum by (le) (rate(tm_trust_compute_seconds_bucket[1m])))"),
                "ingest_delay_p95_ms": 1000 * prom(
                    "histogram_quantile(0.95, sum by (le) (rate(tm_agg_ingest_delay_seconds_bucket[1m])))"),
                "cpu_sensors_pct": cpu.get("sensors", float("nan")),
                "cpu_aggregator_max_pct": max(aggs) if aggs else float("nan"),
                "cpu_aggregators_total_pct": sum(aggs),
                "cpu_mosquitto_pct": cpu.get("mosquitto", float("nan")),
                "cpu_postgres_pct": cpu.get("postgres", float("nan")),
            }
            rows.append(row)
            print("  " + ", ".join(f"{k}={v:.1f}" for k, v in row.items() if k != "sensors"))
    finally:
        print("\nRestoring the fleet size from .env ...")
        compose_up_sensors(None)

    Path("results").mkdir(exist_ok=True)
    with open("results/e5_scalability.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\n  {'sensors':>8}{'offered/s':>11}{'ingested/s':>12}{'latency p95':>13}{'trust p95':>11}{'agg CPU':>9}")
    for r in rows:
        print(f"  {r['sensors']:>8}{r['offered_per_s']:>11.0f}{r['ingested_per_s']:>12.0f}"
              f"{r['window_latency_p95_s']:>12.2f}s{r['trust_compute_p95_ms']:>9.1f}ms{r['cpu_aggregator_max_pct']:>8.0f}%")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        xs = [r["sensors"] for r in rows]
        fig, ax = plt.subplots(1, 3, figsize=(15, 4.5))
        ax[0].plot(xs, [r["offered_per_s"] for r in rows], "k--", label="offered")
        ax[0].plot(xs, [r["ingested_per_s"] for r in rows], marker="o", label="ingested")
        ax[0].set_title("Throughput"); ax[0].set_ylabel("readings / s")
        ax[1].plot(xs, [r["window_latency_p95_s"] for r in rows], marker="o", label="window latency p95 (s)")
        ax[1].plot(xs, [r["trust_compute_p95_ms"] / 1000 for r in rows], marker="o", label="trust compute p95 (s)")
        ax[1].set_title("Latency"); ax[1].set_ylabel("seconds")
        for key, label in [("cpu_sensors_pct", "sensor simulator"), ("cpu_aggregator_max_pct", "busiest aggregator"),
                           ("cpu_mosquitto_pct", "broker"), ("cpu_postgres_pct", "database")]:
            ax[2].plot(xs, [r[key] for r in rows], marker="o", label=label)
        ax[2].set_title("CPU per component"); ax[2].set_ylabel("% of one core")
        for a in ax:
            a.set_xlabel("sensors"); a.grid(alpha=0.3); a.legend()
        fig.tight_layout()
        fig.savefig("results/e5_scalability.png", dpi=150)
        print("\nSaved results/e5_scalability.csv and results/e5_scalability.png\n")
    except ImportError:
        print("\nSaved results/e5_scalability.csv\n")


if __name__ == "__main__":
    main()
