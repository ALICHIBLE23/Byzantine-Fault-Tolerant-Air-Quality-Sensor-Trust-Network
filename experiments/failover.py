"""Failover experiment (E7): crash, stop or pause a region's leader and measure what happens.

Run from the repo root (venv active) while the stack is running:
    python -m experiments.failover --mode kill  --region r-1-1   # crash: lease must expire (TTL)
    python -m experiments.failover --mode stop  --region r-1-1   # graceful: lease released at once
    python -m experiments.failover --mode pause --region r-1-1   # freeze, then wake the old leader up

It reports how long the region was leaderless, whether any 5-second window is missing from the
database, which replica and epoch wrote each window around the failure, and (pause mode) whether
the old leader's stale writes were rejected. Results are appended to results/failover.csv.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("PG_HOST", "localhost")

import psycopg  # noqa: E402
import redis  # noqa: E402

from common.config import load_config  # noqa: E402


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["kill", "stop", "pause"], default="kill")
    ap.add_argument("--region", default="r-1-1")
    ap.add_argument("--pause-seconds", type=float, default=8.0)
    ap.add_argument("--settle", type=float, default=20.0, help="seconds to observe after the failover")
    args = ap.parse_args()
    cfg = load_config("failover-experiment")
    r = redis.Redis(host="localhost", port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=True)
    window_s, grace_s = cfg.window_ms / 1000, cfg.grace_ms / 1000

    old_leader = r.get(f"lease:{args.region}")
    old_epoch = int(r.get(f"epoch:{args.region}") or 0)
    if not old_leader:
        raise SystemExit(f"No leader for {args.region}: is the stack running?")
    print(f"\nRegion {args.region}: leader {old_leader} (epoch {old_epoch}). Action: {args.mode}")

    t0 = time.time()
    if args.mode == "kill":
        compose("kill", old_leader)
    elif args.mode == "stop":
        compose("stop", "-t", "10", old_leader)
    else:
        compose("pause", old_leader)
    t_action = time.time()

    new_leader, t_new = None, None
    while time.time() - t0 < 30:
        holder = r.get(f"lease:{args.region}")
        if holder and holder != old_leader:
            new_leader, t_new = holder, time.time()
            break
        time.sleep(0.05)
    if new_leader is None:
        print("  No takeover within 30 s!")
    else:
        print(f"  New leader {new_leader} (epoch {r.get(f'epoch:{args.region}')}) after "
              f"{t_new - t_action:.2f} s without a leader")

    if args.mode == "pause":
        time.sleep(max(0.0, args.pause_seconds - (time.time() - t_action)))
        compose("unpause", old_leader)
        print(f"  Unpaused {old_leader} after {args.pause_seconds:.0f} s; it still believed it was leader")

    print(f"  Observing for {args.settle:.0f} s ...")
    time.sleep(args.settle + window_s + grace_s)

    start = datetime.fromtimestamp(t0 - 30, tz=timezone.utc)
    end = datetime.fromtimestamp(time.time() - window_s - grace_s - 2, tz=timezone.utc)
    with psycopg.connect(cfg.pg_dsn) as conn:
        rows = conn.execute(
            "SELECT window_start, replica_id, epoch FROM estimates WHERE region = %s AND algorithm = 'trustmesh' "
            "AND window_start BETWEEN %s AND %s ORDER BY window_start", (args.region, start, end)).fetchall()
        fenced_logs = conn.execute(
            "SELECT count(*) FROM logs WHERE message LIKE 'fenced:%%' AND ts > %s",
            (datetime.fromtimestamp(t0, tz=timezone.utc),)).fetchone()[0]
        stepped_down = conn.execute(
            "SELECT count(*) FROM logs WHERE instance = %s AND message LIKE 'lost leader lease%%' AND ts > %s",
            (old_leader, datetime.fromtimestamp(t0, tz=timezone.utc))).fetchone()[0]

    have = {int(ws.timestamp() * 1000) for ws, _, _ in rows}
    first = int(start.timestamp() * 1000)
    first += (-first) % cfg.window_ms
    expected = set(range(first, int(end.timestamp() * 1000) - cfg.window_ms, cfg.window_ms))
    missing = sorted(expected - have)

    print("\n  Windows around the failure (time, written by, epoch):")
    for ws, replica, epoch in rows:
        marker = "  <- failure" if abs(ws.timestamp() - t_action) < window_s else ""
        print(f"    {ws:%H:%M:%S}  {replica:<14} epoch {epoch}{marker}")
    missing_txt = ", ".join(f"{datetime.fromtimestamp(m / 1000):%H:%M:%S}" for m in missing)
    print(f"\n  Missing windows: {len(missing)} of {len(expected)}" + (f"  ({missing_txt})" if missing else ""))
    if args.mode == "pause":
        print(f"  Old leader stepped down after waking: {'yes' if stepped_down else 'no'}")
        print(f"  Stale writes rejected by database fencing: {fenced_logs} batch(es)"
              + ("" if cfg.fencing_self_check is False else
                 "  (0 is expected with FENCING_SELF_CHECK=true: the old leader stops itself first)"))

    if args.mode in ("kill", "stop"):
        compose("start", old_leader)
        time.sleep(5)
        print(f"  Restarted {old_leader}; region {args.region} is now led by {r.get(f'lease:{args.region}')}"
              f" ({old_leader} rejoined as standby)")

    Path("results").mkdir(exist_ok=True)
    out = Path("results/failover.csv")
    new_file = not out.exists()
    with out.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["time", "mode", "region", "old_leader", "new_leader", "leaderless_s",
                        "missing_windows", "expected_windows", "fenced_batches"])
        w.writerow([datetime.now().isoformat(timespec="seconds"), args.mode, args.region, old_leader, new_leader,
                    f"{(t_new - t_action):.2f}" if t_new else "", len(missing), len(expected), fenced_logs])
    print(f"\n  Result appended to {out}\n")


if __name__ == "__main__":
    main()
