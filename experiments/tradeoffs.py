"""Trade-off sweeps (E10), offline with the trust lab (no Docker needed, about a minute).

  1. Flag threshold:   detection rate vs honest sensors wrongly flagged
  2. Reputation speed: how fast on-off attackers are caught vs false flags (alpha_down)
  3. Window size:      accuracy vs latency (each estimate is published window + grace after it starts)

Run from the repo root:  python -m experiments.tradeoffs
Results: results/e10_tradeoffs.csv and results/e10_tradeoffs.png
"""
from __future__ import annotations

import csv
import dataclasses
import statistics
from pathlib import Path

from common.config import load_config
from common.trust import TrustParams
from experiments.trust_lab import simulate

MIX = "offset_high:0.1,hider:0.05,stuck:0.05"
MINUTES = 10


def main() -> None:
    cfg = load_config("tradeoffs")
    rows = []

    print("\n1. Flag threshold (mix: %s)" % MIX)
    print(f"   {'threshold':>9}{'detected':>10}{'false flags':>13}{'MAE':>7}")
    for theta in [0.2, 0.3, 0.4, 0.5, 0.6]:
        r = simulate(cfg, MIX, MINUTES, TrustParams(flag_threshold=theta, unflag_threshold=theta + 0.1))
        rows.append({"sweep": "flag_threshold", "value": theta, "detected": r.detection_rate,
                     "false_flags": r.false_flag_rate, "mae": r.mae["trustmesh"], "detect_s": float("nan")})
        print(f"   {theta:>9.1f}{r.detection_rate:>10.1%}{r.false_flag_rate:>13.2%}{r.mae['trustmesh']:>7.2f}")

    print("\n2. Reputation loss speed alpha_down (mix: on_off:0.2)")
    print(f"   {'alpha_down':>10}{'detected':>10}{'median detect':>15}{'false flags':>13}")
    for alpha in [0.1, 0.2, 0.3, 0.5]:
        r = simulate(cfg, "on_off:0.2", MINUTES, TrustParams(alpha_down=alpha))
        t = r.detect_seconds.get("on_off", float("nan"))
        rows.append({"sweep": "alpha_down", "value": alpha, "detected": r.detection_rate,
                     "false_flags": r.false_flag_rate, "mae": r.mae["trustmesh"], "detect_s": t})
        print(f"   {alpha:>10.1f}{r.detection_rate:>10.1%}{t:>14.0f}s{r.false_flag_rate:>13.2%}")

    print("\n3. Window size (mix: %s)" % MIX)
    print(f"   {'window':>8}{'latency':>10}{'MAE':>7}{'false flags':>13}")
    for window_ms in [2000, 5000, 10000, 15000]:
        c = dataclasses.replace(cfg, window_ms=window_ms)
        r = simulate(c, MIX, MINUTES)
        latency = (window_ms + cfg.grace_ms) / 1000
        rows.append({"sweep": "window_ms", "value": window_ms, "detected": r.detection_rate,
                     "false_flags": r.false_flag_rate, "mae": r.mae["trustmesh"], "detect_s": latency})
        print(f"   {window_ms / 1000:>7.0f}s{latency:>9.0f}s{r.mae['trustmesh']:>7.2f}{r.false_flag_rate:>13.2%}")

    Path("results").mkdir(exist_ok=True)
    with open("results/e10_tradeoffs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(1, 3, figsize=(15, 4.5))
        th = [r for r in rows if r["sweep"] == "flag_threshold"]
        ax[0].plot([100 * r["false_flags"] for r in th], [100 * r["detected"] for r in th], marker="o")
        for k, r in enumerate(th):   # staggered labels: neighbouring points overlap
            ax[0].annotate(f"θ={r['value']:.1f}", (100 * r["false_flags"], 100 * r["detected"]),
                           textcoords="offset points", xytext=(4, 8 + 12 * (k % 3)),
                           arrowprops={"arrowstyle": "-", "lw": 0.5})
        ax[0].set_xlabel("honest sensors wrongly flagged (%)"); ax[0].set_ylabel("faulty sensors detected (%)")
        ax[0].set_title("Sensitivity vs false accusations")
        al = [r for r in rows if r["sweep"] == "alpha_down"]
        ax[1].plot([r["value"] for r in al], [r["detect_s"] for r in al], marker="o", label="median time to detect (s)")
        ax1b = ax[1].twinx()
        ax1b.plot([r["value"] for r in al], [100 * r["false_flags"] for r in al], marker="s", color="tab:red",
                  label="false flags (%)")
        ax[1].set_xlabel("alpha_down (reputation loss rate)"); ax[1].set_ylabel("seconds")
        ax1b.set_ylabel("false flags (%)", color="tab:red")
        ax[1].set_title("On-off attackers: speed vs stability")
        wi = [r for r in rows if r["sweep"] == "window_ms"]
        ax[2].plot([r["detect_s"] for r in wi], [r["mae"] for r in wi], marker="o")
        for r in wi:
            ax[2].annotate(f"{r['value'] / 1000:.0f} s window", (r["detect_s"], r["mae"]),
                           textcoords="offset points", xytext=(5, 5))
        ax[2].set_xlabel("latency until the estimate exists (s)"); ax[2].set_ylabel("MAE (ug/m3)")
        ax[2].set_title("Window size: latency vs accuracy")
        for a in ax:
            a.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig("results/e10_tradeoffs.png", dpi=150)
        print("\nSaved results/e10_tradeoffs.csv and results/e10_tradeoffs.png\n")
    except ImportError:
        print("\nSaved results/e10_tradeoffs.csv\n")


if __name__ == "__main__":
    main()
