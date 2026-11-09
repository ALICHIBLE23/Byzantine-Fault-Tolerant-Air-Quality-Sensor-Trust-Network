"""Stage 2 check: does the live system catch the faulty sensors, and is TrustMesh more accurate?

Set a behavior mix in .env first (e.g. BEHAVIOR_MIX=offset_high:0.1,hider:0.05,stuck:0.05),
restart the stack, wait 3-4 minutes, then run from the repo root:
    python -m experiments.check_stage2
"""
from __future__ import annotations

import os
import statistics
from collections import Counter, defaultdict

os.environ.setdefault("PG_HOST", "localhost")

import psycopg  # noqa: E402

from common.config import load_config  # noqa: E402
from common.truth import truth  # noqa: E402

results: list[bool] = []


def check(passed: bool, message: str) -> None:
    results.append(passed)
    print(f"  [{'OK' if passed else 'FAIL':4}] {message}")


def main() -> None:
    cfg = load_config("check-stage2")
    with psycopg.connect(cfg.pg_dsn) as conn:
        sensors = conn.execute("SELECT sensor_id, region, x_km, y_km, behavior FROM sensors").fetchall()
        trust = dict(conn.execute("SELECT sensor_id, flagged FROM sensor_trust").fetchall())
        reasons = dict(conn.execute("SELECT sensor_id, reason FROM sensor_trust WHERE flagged").fetchall())
        # Time from the start of the current sensor run (registered_at) to the first flag.
        first_flag = dict(conn.execute(
            "SELECT h.sensor_id, EXTRACT(EPOCH FROM min(h.window_start) - s.registered_at) "
            "FROM trust_history h JOIN sensors s USING (sensor_id) "
            "WHERE h.flagged AND h.window_start >= s.registered_at "
            "GROUP BY h.sensor_id, s.registered_at").fetchall())
        estimates = conn.execute(
            "SELECT region, window_start, window_ms, algorithm, estimate FROM estimates "
            "WHERE window_start > now() - interval '3 minutes'").fetchall()

    if not trust:
        print("No trust data yet. Did you apply 02-trust.sql and wait a minute? See the Stage 2 notes.")
        return

    labels = {sid: behavior for sid, _, _, _, behavior in sensors}
    faulty = {sid for sid, b in labels.items() if b != "honest"}
    honest = set(labels) - faulty
    flagged = {sid for sid, f in trust.items() if f}

    print(f"\n1. Detection  ({len(faulty)} faulty of {len(labels)} sensors configured)")
    if faulty:
        caught = flagged & faulty
        precision = len(caught) / len(flagged) if flagged else 0.0
        recall = len(caught) / len(faulty)
        check(recall >= 0.85, f"detected {len(caught)}/{len(faulty)} faulty sensors (recall {recall:.0%})")
        check(precision >= 0.9, f"{len(flagged)} flagged, of which {len(caught)} really faulty (precision {precision:.0%})")
        print(f"\n  {'behavior':<12} {'detected':>9} {'median time':>12}   (time since the sensors started)")
        by_type = defaultdict(list)
        for sid in faulty:
            by_type[labels[sid]].append(sid)
        for behavior, sids in sorted(by_type.items()):
            hit = [s for s in sids if s in flagged]
            times = [float(first_flag[s]) for s in sids if s in first_flag]
            t = f"{statistics.median(times):8.0f} s" if times else "       n/a"
            print(f"  {behavior:<12} {len(hit):4d}/{len(sids):<4d} {t:>12}")
        missed = sorted(faulty - flagged)
        if missed:
            print(f"\n  Missed: {', '.join(f'{s} ({labels[s]})' for s in missed[:10])}"
                  f"{' ...' if len(missed) > 10 else ''}")
    false_flags = flagged & honest
    rate = len(false_flags) / len(honest) if honest else 0.0
    check(rate <= 0.03, f"{len(false_flags)} honest sensors flagged ({rate:.1%} false-flag rate)")
    if false_flags:
        print(f"  Reasons for false flags: {dict(Counter(reasons.get(s, '') for s in false_flags))}")

    print("\n2. Accuracy over the last 3 minutes (MAE vs ground truth, ug/m3)")
    positions = defaultdict(list)
    for _, region, x, y, _ in sensors:
        positions[region].append((x, y))
    cache: dict[tuple, float] = {}
    errors = defaultdict(list)
    for region, ws, wms, algorithm, estimate in estimates:
        key = (region, ws)
        if key not in cache:
            mid = ws.timestamp() + wms / 2000
            cache[key] = statistics.fmean(truth(x, y, mid, cfg) for x, y in positions[region])
        errors[algorithm].append(abs(estimate - cache[key]))
    if not errors:
        print("  No recent estimates.")
        return
    mae = {a: statistics.fmean(e) for a, e in errors.items()}
    for algorithm in sorted(mae, key=mae.get):
        print(f"    {algorithm:<13} {mae[algorithm]:6.2f}")
    if "trustmesh" in mae:
        best_baseline = min(v for k, v in mae.items() if k != "trustmesh")
        if faulty:
            check(mae["trustmesh"] <= best_baseline, "TrustMesh is the most accurate method under attack")
        else:
            check(mae["trustmesh"] < 1.0, "TrustMesh stays accurate with all sensors honest")

    print(f"\n{'All checks passed: Stage 2 complete.' if all(results) else 'Some checks failed.'}\n")


if __name__ == "__main__":
    main()
