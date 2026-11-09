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
from common.trust import TrustEngine, TrustParams
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


def simulate(cfg: Config, mix: str, minutes: float = 10.0, params: TrustParams | None = None,
             warmup_s: float = 60.0, t0: float = 1_700_000_000.0) -> ScenarioResult:
    fleet = build_fleet(cfg, mix)
    rngs = [random.Random(cfg.seed * 1_000_003 + s.index) for s in fleet]
    behaviors = [make_behavior(s.behavior, cfg, rngs[i], t0) for i, s in enumerate(fleet)]
    offsets = [random.Random(s.index).uniform(0, cfg.publish_interval_s) for s in fleet]
    engines = defaultdict(lambda: TrustEngine(params))
    regions = sorted({s.region for s in fleet})
    by_region = {r: [i for i, s in enumerate(fleet) if s.region == r] for r in regions}

    window_s = cfg.window_ms / 1000
    n_windows = int(minutes * 60 / window_s)
    errors = defaultdict(list)
    first_flag: dict[str, float] = {}
    false_flags_over_time = []
    honest_ids = {s.sensor_id for s in fleet if s.behavior == "honest"}

    for w in range(n_windows):
        ws = t0 + w * window_s
        flagged_now: set[str] = set()
        for region in regions:
            idx = by_region[region]
            ids, xs, ys, vals = [], [], [], []
            for i in idx:
                s = fleet[i]
                readings = []
                t = ws + (offsets[i] - ws) % cfg.publish_interval_s
                while t < ws + window_s:
                    v = behaviors[i].observe(truth(s.x_km, s.y_km, t, cfg), t, rngs[i])
                    if v is not None:
                        readings.append(round(v, 3))
                    t += cfg.publish_interval_s
                if readings:
                    ids.append(s.sensor_id); xs.append(s.x_km); ys.append(s.y_km)
                    vals.append(sum(readings) / len(readings))
            if not vals:
                continue
            values = np.array(vals)
            result = engines[region].process_window(ids, xs, ys, values)
            for v in result.verdicts:
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
    final_flagged = {sid for e in engines.values() for sid, st in e.states.items() if st.flagged}
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
    args = ap.parse_args()
    cfg = load_config("trust-lab")
    if args.sweep:
        sweep(cfg, args.attack, args.minutes, [float(x) for x in args.fractions.split(",")])
    else:
        started = time.perf_counter()
        r = simulate(cfg, args.mix, args.minutes)
        print_scenario(r, time.perf_counter() - started)


if __name__ == "__main__":
    main()
