"""Stage 5 check: quorum alerting, and how accurate alerts are compared with the baselines.

For every decided window it compares three things against the ground truth (true regional value
above the threshold or not):
  * the system's quorum decision (alarm or not),
  * what each estimate would have alerted on its own (mean, median, trimmed mean, TrustMesh).
Run from the repo root after the stack has run for 10+ minutes (with a BEHAVIOR_MIX set):
    python -m experiments.check_stage5 [--minutes 15]
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import urllib.parse
import urllib.request
from collections import defaultdict

os.environ.setdefault("PG_HOST", "localhost")

import psycopg  # noqa: E402

from common.config import load_config  # noqa: E402
from common.truth import truth  # noqa: E402

PROM = f"http://localhost:{os.getenv('PROMETHEUS_PORT', '9090')}"
results: list[bool] = []


def check(passed: bool, message: str) -> None:
    results.append(passed)
    print(f"  [{'OK' if passed else 'FAIL':4}] {message}")


def prom(query: str) -> list[dict]:
    with urllib.request.urlopen(f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": query}), timeout=3) as r:
        return json.load(r)["data"]["result"]


def scores(predicted: dict, actual: dict) -> tuple[int, int, int, float, float]:
    keys = predicted.keys() & actual.keys()
    tp = sum(predicted[k] and actual[k] for k in keys)
    fp = sum(predicted[k] and not actual[k] for k in keys)
    fn = sum(not predicted[k] and actual[k] for k in keys)
    precision = tp / (tp + fp) if tp + fp else float("nan")
    recall = tp / (tp + fn) if tp + fn else float("nan")
    return tp, fp, fn, precision, recall


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=15)
    args = ap.parse_args()
    cfg = load_config("check-stage5")
    threshold = cfg.alert_threshold
    since = f"now() - interval '{args.minutes} minutes'"

    print("\n1. Votes and decisions")
    votes = {x["metric"]["region"]: float(x["value"][1]) for x in prom("sum by (region) (rate(tm_alert_votes_total[1m]))")}
    per_window = cfg.alert_replicas / (cfg.window_ms / 1000)
    for region in sorted(votes):
        check(votes[region] > 0.8 * per_window,
              f"{region}: {votes[region]:.2f} votes/s (expected {per_window:.2f}: {cfg.alert_replicas} replicas per window)")
    with psycopg.connect(cfg.pg_dsn) as conn:
        decisions = conn.execute(
            f"SELECT region, window_start, decision, votes_over, votes_total, voters FROM alert_decisions "
            f"WHERE window_start > {since} ORDER BY window_start").fetchall()
        estimates = conn.execute(
            f"SELECT region, window_start, window_ms, algorithm, estimate FROM estimates "
            f"WHERE window_start > {since}").fetchall()
        sensors = conn.execute("SELECT region, x_km, y_km FROM sensors").fetchall()
        alerts = conn.execute(
            f"SELECT region, status, opened_window, closed_window, peak_estimate, votes_over, votes_total "
            f"FROM alerts WHERE opened_window > {since} ORDER BY opened_window").fetchall()
    if not decisions:
        print("  No decisions yet: is the alerting service running? (docker compose logs alerting)")
        return
    counts = defaultdict(int)
    for _, _, decision, _, total, _ in decisions:
        counts[decision] += 1
    complete = sum(1 for d in decisions if d[4] >= cfg.alert_replicas) / len(decisions)
    print(f"  [INFO] {len(decisions)} windows decided: {dict(counts)}; quorum {cfg.alert_quorum} of {cfg.alert_replicas}")
    check(complete > 0.95, f"{complete:.1%} of windows received votes from every replica")
    blocked_by = defaultdict(int)
    for *_, decision, _, _, voters in decisions:
        if decision == "blocked":
            for v in voters.split(","):
                blocked_by[v] += 1
    if blocked_by:
        print(f"  [INFO] lone 'over' votes blocked by the quorum, by replica: {dict(blocked_by)}")

    print(f"\n2. Alerts in the last {args.minutes} minutes (threshold {threshold} ug/m3)")
    for region, status, opened, closed, peak, n_over, n_total in alerts:
        print(f"     {region}  {opened:%H:%M:%S} -> {closed:%H:%M:%S}" if closed else
              f"     {region}  {opened:%H:%M:%S} -> (open)", f" peak {peak:.1f}, votes {n_over}/{n_total}, {status}")
    if not alerts:
        print("     none (the truth may not have crossed the threshold; see section 3)")

    print("\n3. Alert accuracy per window against the ground truth")
    positions = defaultdict(list)
    for region, x, y in sensors:
        positions[region].append((x, y))
    actual, cache = {}, {}
    window_s = cfg.window_ms / 1000
    for region, ws, *_ in decisions:
        key = (region, ws)
        mid = ws.timestamp() + window_s / 2
        cache[key] = statistics.fmean(truth(x, y, mid, cfg) for x, y in positions[region])
        actual[key] = cache[key] > threshold
    predicted = {"system (quorum)": {(r, ws): d == "alarm" for r, ws, d, *_ in decisions}}
    for region, ws, _, algorithm, estimate in estimates:
        predicted.setdefault(f"{algorithm} alone", {})[(region, ws)] = estimate > threshold
    above = sum(actual.values())
    print(f"     truth above the threshold in {above} of {len(actual)} windows\n")
    print(f"     {'method':<22}{'correct alarms':>15}{'false alarms':>14}{'missed':>8}{'precision':>11}{'recall':>8}")
    table = {}
    for name in ["system (quorum)", "trustmesh alone", "mean alone", "median alone", "trimmed_mean alone"]:
        if name not in predicted:
            continue
        tp, fp, fn, p, r = scores(predicted[name], actual)
        table[name] = (tp, fp, fn)
        fmt = lambda v: "   n/a" if v != v else f"{v:6.0%}"
        print(f"     {name:<22}{tp:>15}{fp:>14}{fn:>8}{fmt(p):>11}{fmt(r):>8}")
    if "mean alone" in table:
        check(table["system (quorum)"][1] <= table["mean alone"][1],
              "the system raises no more false alarms than alerting on the plain mean")
    print(f"\n{'All checks passed: Stage 5 complete.' if all(results) else 'Some checks failed.'}\n")


if __name__ == "__main__":
    main()
