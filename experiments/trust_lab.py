"""Offline trust lab: evaluates the trust algorithm without Docker, broker or database.

It simulates the sensor fleet (same fleet, behaviors and ground truth as the live system),
groups readings into windows exactly like the aggregator, and runs TrustMesh next to the
mean / median / trimmed-mean baselines on identical data.

Examples (from the repo root, venv active):
    python -m experiments.trust_lab --mix "offset_high:0.2"            # one scenario, detailed report
    python -m experiments.trust_lab --mix "on_off:0.15" --minutes 15
    python -m experiments.trust_lab --sweep                              # accuracy vs % faulty, plot
    python -m experiments.trust_lab --sweep --attack hider
"""
from __future__ import annotations

import argparse
import random
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np

from common.aggregation import ALGORITHMS
from common.config import Config, load_config
from common.geo import distance_to_own_edge, distance_to_region, regions_within
from common.trust import TrustEngine, TrustParams, update_reputation
from common.truth import truth
from services.sensors.behaviors import make_behavior
from services.sensors.fleet import build_fleet

FAULT_TYPES = ["offset_high", "hider", "on_off", "stuck", "drift", "spike"]


@dataclass
class ScenarioResult:
    mix: str
    mae: dict[str, float]
    detection_rate: float
    false_flag_rate: float
    detect_seconds: dict[str, float]
    detected_by_type: dict[str, tuple[int, int]]
    false_flags_over_time: list[float] = field(default_factory=list)
    edge: dict[str, tuple[int, int]] = field(default_factory=dict)   # (errors, sensors) for edge / interior


EDGE_KM = 0.3  # sensors closer than this to their region's boundary count as "edge" sensors


def simulate(cfg: Config, mix: str, minutes: float = 10.0, params: TrustParams | None = None,
             warmup_s: float = 60.0, t0: float = 1_700_000_000.0, border: bool = True) -> ScenarioResult:
    """border=True mirrors Stage 4: each region also sees neighbours' border sensors as context, and
    border sensors share one reputation updated by every region that evaluates them."""
    fleet = build_fleet(cfg, mix)
    rngs = [random.Random(cfg.seed * 1_000_003 + s.index) for s in fleet]
    behaviors = [make_behavior(s.behavior, cfg, rngs[i], t0) for i, s in enumerate(fleet)]
    offsets = [random.Random(s.index).uniform(0, cfg.publish_interval_s) for s in fleet]
    engines = defaultdict(lambda: TrustEngine(params))
    regions = sorted({s.region for s in fleet})
    by_region = {r: [i for i, s in enumerate(fleet) if s.region == r] for r in regions}
    radius = (params or TrustParams()).radius_km
    members = {r: [(i, True) for i in by_region[r]] for r in regions}
    is_border, evaluators = {}, {}
    for i, s in enumerate(fleet):
        neighbours = regions_within(s.x_km, s.y_km, cfg, radius)
        is_border[s.sensor_id] = border and bool(neighbours)
        evaluators[s.sensor_id] = 1 + len(neighbours)
        if border:
            for r in regions:
                if r != s.region and distance_to_region(s.x_km, s.y_km, r, cfg) <= radius:
                    members[r].append((i, False))
    shared: dict[str, float] = {}
    p_used = params or TrustParams()

    window_s = cfg.window_ms / 1000
    n_windows = int(minutes * 60 / window_s)
    errors = defaultdict(list)
    first_flag: dict[str, float] = {}
    false_flags_over_time = []
    honest_ids = {s.sensor_id for s in fleet if s.behavior == "honest"}

    for w in range(n_windows):
        ws = t0 + w * window_s
        flagged_now: set[str] = set()
        window_values: dict[int, float] = {}   # each sensor reports once per window, whoever listens
        for i, s in enumerate(fleet):
            readings = []
            t = ws + (offsets[i] - ws) % cfg.publish_interval_s
            while t < ws + window_s:
                v = behaviors[i].observe(truth(s.x_km, s.y_km, t, cfg), t, rngs[i])
                if v is not None:
                    readings.append(round(v, 3))
                t += cfg.publish_interval_s
            if readings:
                window_values[i] = sum(readings) / len(readings)
        for region in regions:
            idx = by_region[region]
            present = [(i, own) for i, own in members[region] if i in window_values]
            if not any(own for _, own in present):
                continue
            ids = [fleet[i].sensor_id for i, _ in present]
            xs = [fleet[i].x_km for i, _ in present]
            ys = [fleet[i].y_km for i, _ in present]
            owned = [own for _, own in present]
            all_values = np.array([window_values[i] for i, _ in present])
            values = all_values[np.array(owned)]
            result = engines[region].process_window(
                ids, xs, ys, all_values, owned=owned,
                shared={sid: shared.get(sid) for sid in ids if is_border[sid]})
            for v in result.verdicts:          # atomic shared update: every region's evidence counts
                if is_border[v.sensor_id]:
                    merged = update_reputation(shared.get(v.sensor_id, v.prior_reputation), v.score, p_used,
                                               1.0 / evaluators[v.sensor_id])
                    shared[v.sensor_id] = merged
                    engines[region].state(v.sensor_id).reputation = merged
            for v in result.verdicts:
                if not v.owned:
                    continue
                if v.flagged:
                    flagged_now.add(v.sensor_id)
                    first_flag.setdefault(v.sensor_id, ws - t0)
            if ws - t0 >= warmup_s:
                mid = ws + window_s / 2
                true_val = statistics.fmean(truth(fleet[i].x_km, fleet[i].y_km, mid, cfg) for i in idx)
                errors["trustmesh"].append(abs(result.estimate - true_val))
                for name, fn in ALGORITHMS.items():
                    errors[name].append(abs(fn(values) - true_val))
        if honest_ids:
            false_flags_over_time.append(len(flagged_now & honest_ids) / len(honest_ids))

    faulty = [s for s in fleet if s.behavior != "honest"]
    final_flagged = {sid for r, e in engines.items() for sid, st in e.states.items()
                     if st.flagged and sid in {fleet[i].sensor_id for i in by_region[r]}}
    edge = {"edge": [0, 0], "interior": [0, 0]}
    for s in fleet:
        bucket = "edge" if distance_to_own_edge(s.x_km, s.y_km, cfg) < EDGE_KM else "interior"
        wrong = (s.sensor_id in final_flagged) != (s.behavior != "honest")   # false flag or miss
        edge[bucket][0] += wrong
        edge[bucket][1] += 1
    ever_flagged = set(first_flag)
    detected_by_type = defaultdict(lambda: [0, 0])
    detect_times = defaultdict(list)
    for s in faulty:
        detected_by_type[s.behavior][1] += 1
        if s.sensor_id in ever_flagged:
            detected_by_type[s.behavior][0] += 1
            detect_times[s.behavior].append(first_flag[s.sensor_id])
    late = false_flags_over_time[int(warmup_s / window_s):] or [0.0]
    return ScenarioResult(
        mix=mix or "none",
        mae={k: statistics.fmean(v) for k, v in errors.items()},
        detection_rate=(sum(s.sensor_id in ever_flagged for s in faulty) / len(faulty)) if faulty else 1.0,
        false_flag_rate=statistics.fmean(late),
        detect_seconds={k: statistics.median(v) for k, v in detect_times.items()},
        detected_by_type={k: tuple(v) for k, v in detected_by_type.items()},
        false_flags_over_time=false_flags_over_time,
        edge={k: tuple(v) for k, v in edge.items()},
    )


def print_scenario(r: ScenarioResult, elapsed: float) -> None:
    print(f"\nScenario: {r.mix}   ({elapsed:.1f}s to simulate)")
    print("  Accuracy (mean absolute error vs ground truth, ug/m3):")
    for name in ["trustmesh", *ALGORITHMS]:
        print(f"    {name:<13} {r.mae[name]:6.2f}")
    print(f"  Faulty sensors detected:   {r.detection_rate:6.1%}")
    print(f"  Honest sensors flagged:    {r.false_flag_rate:6.2%}  (average after warm-up)")
    for kind, (hit, total) in sorted(r.detected_by_type.items()):
        t = r.detect_seconds.get(kind)
        print(f"    {kind:<12} {hit:3d}/{total:<3d} detected" + (f", median {t:5.0f}s after start" if t is not None else ""))
    for bucket, (wrong, total) in r.edge.items():
        print(f"  Misjudged {bucket} sensors (<{EDGE_KM} km from boundary)" if bucket == "edge" else
              f"  Misjudged {bucket} sensors", f"{wrong}/{total} ({wrong / max(total, 1):.1%})")


def sweep(cfg: Config, attack: str, minutes: float, fractions: list[float]) -> None:
    rows = []
    for f in fractions:
        if attack == "mixed":
            mix = ",".join(f"{a}:{f / len(FAULT_TYPES):.4f}" for a in FAULT_TYPES) if f > 0 else ""
        else:
            mix = f"{attack}:{f}" if f > 0 else ""
        started = time.perf_counter()
        r = simulate(cfg, mix, minutes)
        rows.append((f, r))
        print(f"  {f:4.0%} faulty  ({time.perf_counter() - started:4.1f}s)  " +
              "  ".join(f"{k}={v:5.2f}" for k, v in r.mae.items()) +
              f"  detected={r.detection_rate:5.1%}  false_flags={r.false_flag_rate:5.2%}")

    print(f"\n  {'faulty':>6}  " + "".join(f"{n:>13}" for n in ["trustmesh", *ALGORITHMS]) + f"{'detected':>10}{'false flags':>12}")
    for f, r in rows:
        print(f"  {f:6.0%}  " + "".join(f"{r.mae[n]:13.2f}" for n in ["trustmesh", *ALGORITHMS]) +
              f"{r.detection_rate:10.1%}{r.false_flag_rate:12.2%}")

    try:
        import os
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        os.makedirs("results", exist_ok=True)
        fig, ax = plt.subplots(figsize=(8, 5))
        styles = {"trustmesh": dict(lw=3, color="tab:green"), "mean": dict(ls="--"),
                  "median": dict(ls="--"), "trimmed_mean": dict(ls="--")}
        for name in ["trustmesh", *ALGORITHMS]:
            ax.plot([f * 100 for f, _ in rows], [r.mae[name] for _, r in rows], marker="o", label=name, **styles[name])
        ax.set_xlabel("Faulty sensors (%)")
        ax.set_ylabel("Mean absolute error (ug/m3)")
        ax.set_title(f"Estimate error vs faulty sensors ({attack} attack)")
        ax.grid(alpha=0.3)
        ax.legend()
        path = f"results/e1_error_vs_faulty_{attack}.png"
        fig.tight_layout()
        fig.savefig(path, dpi=150)
        print(f"\n  Plot saved to {path}")
    except ImportError:
        print("\n  (matplotlib not installed: skipping plot)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mix", default="", help="behavior mix, e.g. 'offset_high:0.2,stuck:0.05'")
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--sweep", action="store_true", help="sweep the faulty fraction and plot")
    ap.add_argument("--attack", default="mixed", choices=["mixed", *FAULT_TYPES])
    ap.add_argument("--fractions", default="0,0.1,0.2,0.3,0.4,0.5")
    ap.add_argument("--no-border", action="store_true", help="disable border exchange (Stage 2-3 behaviour)")
    args = ap.parse_args()
    cfg = load_config("trust-lab")
    if args.sweep:
        sweep(cfg, args.attack, args.minutes, [float(x) for x in args.fractions.split(",")])
    else:
        started = time.perf_counter()
        r = simulate(cfg, args.mix, args.minutes, border=not args.no_border)
        print_scenario(r, time.perf_counter() - started)


if __name__ == "__main__":
    main()
