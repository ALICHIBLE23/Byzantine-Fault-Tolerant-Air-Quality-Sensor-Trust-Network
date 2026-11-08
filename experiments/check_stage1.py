"""Stage 1 end-to-end check: is data flowing, and are the estimates correct?

Run from the repo root with the virtualenv active, after the stack has run for ~30 seconds:
    python -m experiments.check_stage1
"""
from __future__ import annotations

import json
import os
import urllib.request
from collections import defaultdict

os.environ.setdefault("PG_HOST", "localhost")  # this script runs on the host, not in Docker

import psycopg  # noqa: E402

from common.config import load_config  # noqa: E402
from common.truth import truth  # noqa: E402

PROMETHEUS = f"http://localhost:{os.getenv('PROMETHEUS_PORT', '9090')}"
results: list[bool] = []


def check(passed: bool, message: str) -> None:
    results.append(passed)
    print(f"  [{'OK' if passed else 'FAIL':4}] {message}")


def main() -> None:
    cfg = load_config("check-stage1")

    print("\n1. Prometheus scrape targets")
    try:
        with urllib.request.urlopen(f"{PROMETHEUS}/api/v1/targets", timeout=3) as resp:
            targets = json.load(resp)["data"]["activeTargets"]
        for t in sorted(targets, key=lambda t: t["labels"]["job"]):
            check(t["health"] == "up", f"{t['labels']['job']:<12} {t['scrapeUrl']}  health={t['health']}")
    except OSError as exc:
        check(False, f"cannot reach Prometheus at {PROMETHEUS}: {exc}")

    print("\n2. Data in PostgreSQL")
    with psycopg.connect(cfg.pg_dsn) as conn:
        sensors = conn.execute("SELECT sensor_id, region, x_km, y_km FROM sensors").fetchall()
        recent, span = conn.execute(
            "SELECT count(*), COALESCE(EXTRACT(EPOCH FROM max(received_at) - min(received_at)), 0) "
            "FROM readings WHERE received_at > now() - interval '30 seconds'").fetchone()
        estimates = conn.execute(
            "SELECT region, window_start, window_ms, algorithm, estimate, n_sensors FROM estimates "
            "WHERE window_start > now() - interval '3 minutes' ORDER BY window_start").fetchall()
        log_services = dict(conn.execute("SELECT service, count(*) FROM logs GROUP BY service").fetchall())
        errors = conn.execute(
            "SELECT count(*) FROM logs WHERE level IN ('ERROR', 'CRITICAL') "
            "AND ts > now() - interval '10 minutes'").fetchone()[0]

    check(len(sensors) == cfg.num_sensors, f"{len(sensors)} sensors registered (expected {cfg.num_sensors})")
    expected_rate = cfg.num_sensors / cfg.publish_interval_s
    rate = recent / float(span) if span and span > 1 else 0.0  # divide by the real span, not 30 s
    check(rate > 0.8 * expected_rate, f"{rate:.0f} readings/s stored (expected ~{expected_rate:.0f})")
    check(len(estimates) > 0, f"{len(estimates)} estimate rows in the last 3 minutes")
    check({"sensors", "aggregator"} <= set(log_services),
          f"logs received from: {', '.join(f'{s} ({n})' for s, n in sorted(log_services.items()))}")
    check(errors == 0, f"{errors} ERROR log lines in the last 10 minutes")

    if not estimates:
        print("\nNo estimates yet: wait 30 seconds and run again.")
        return

    print("\n3. Accuracy against ground truth (true value = average truth at the region's sensors)")
    positions = defaultdict(list)
    for _, region, x, y in sensors:
        positions[region].append((x, y))
    truth_cache: dict[tuple, float] = {}
    abs_errors = defaultdict(list)
    latest: dict[str, tuple] = {}
    for region, window_start, window_ms, algorithm, estimate, n_sensors in estimates:
        key = (region, window_start)
        if key not in truth_cache:
            mid = window_start.timestamp() + window_ms / 2000
            pts = positions[region]
            truth_cache[key] = sum(truth(x, y, mid, cfg) for x, y in pts) / len(pts)
        abs_errors[algorithm].append(abs(estimate - truth_cache[key]))
        if algorithm == "mean":
            latest[region] = (window_start, estimate, truth_cache[key], n_sensors)

    print(f"\n  {'region':<8} {'window':<10} {'estimate':>9} {'truth':>8} {'error':>7} {'sensors':>8}")
    for region in sorted(latest):
        ws, est, tv, n = latest[region]
        print(f"  {region:<8} {ws:%H:%M:%S}  {est:9.2f} {tv:8.2f} {est - tv:7.2f} {n:8d}")
    print()
    for algorithm, errs in sorted(abs_errors.items()):
        mae = sum(errs) / len(errs)
        line = f"{algorithm:<13} mean absolute error {mae:.2f} ug/m3 over {len(errs)} windows"
        if algorithm == "mean":
            check(mae < 1.0, line)
        else:
            # With honest sensors only, mean is the unbiased estimator of the regional average.
            # Median and trimmed mean are biased low because plumes skew the distribution:
            # this is the price of robustness, measured here as the baseline for later stages.
            print(f"  [INFO] {line}  (bias expected: plumes skew the readings)")

    print(f"\n{'All checks passed: Stage 1 complete.' if all(results) else 'Some checks failed.'}\n")


if __name__ == "__main__":
    main()
