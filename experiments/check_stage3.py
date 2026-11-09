"""Stage 3 check: replication, leadership, shared trust state and write-once semantics.

Run from the repo root with the stack up for at least a minute:
    python -m experiments.check_stage3
"""
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from collections import defaultdict

os.environ.setdefault("PG_HOST", "localhost")

import psycopg  # noqa: E402
import redis  # noqa: E402

from common.config import load_config  # noqa: E402
from common.geo import all_regions  # noqa: E402

PROM = f"http://localhost:{os.getenv('PROMETHEUS_PORT', '9090')}"
results: list[bool] = []


def check(passed: bool, message: str) -> None:
    results.append(passed)
    print(f"  [{'OK' if passed else 'FAIL':4}] {message}")


def prom(query: str) -> list[dict]:
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=3) as resp:
        return json.load(resp)["data"]["result"]


def main() -> None:
    cfg = load_config("check-stage3")
    regions = all_regions(cfg)
    r = redis.Redis(host="localhost", port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=True)

    print("\n1. Replicas")
    targets = [t for t in prom('up{job="aggregators"}')]
    up = sorted(t["metric"]["instance"].split(":")[0] for t in targets if t["value"][1] == "1")
    check(len(up) == 4, f"{len(up)}/4 aggregator replicas up: {', '.join(up)}")

    print("\n2. Exactly one leader per region")
    leaders_metric = {x["metric"]["region"]: float(x["value"][1]) for x in prom("sum by (region) (tm_agg_is_leader)")}
    led_by = defaultdict(list)
    for x in prom("tm_agg_is_leader == 1"):
        led_by[x["metric"]["instance"].split(":")[0]].append(x["metric"]["region"])
    for region in regions:
        holder = r.get(f"lease:{region}")
        epoch = r.get(f"epoch:{region}")
        check(leaders_metric.get(region) == 1 and holder is not None,
              f"{region}: lease held by {holder} (epoch {epoch}), replicas claiming leadership: "
              f"{int(leaders_metric.get(region, 0))}")
    print("     partitioning: " + "; ".join(f"{rep} leads {', '.join(sorted(rs))}" for rep, rs in sorted(led_by.items())))

    print("\n3. Fencing epochs published to the database")
    with psycopg.connect(cfg.pg_dsn) as conn:
        db_epochs = dict(conn.execute("SELECT region, epoch FROM region_epochs").fetchall())
        sensors_per_region = dict(conn.execute("SELECT region, count(*) FROM sensors GROUP BY region").fetchall())
        rate = conn.execute(
            "SELECT count(*) / GREATEST(EXTRACT(EPOCH FROM max(received_at) - min(received_at)), 1) "
            "FROM readings WHERE received_at > now() - interval '30 seconds'").fetchone()[0]
        writers = conn.execute(
            "SELECT region, count(DISTINCT replica_id) FROM estimates "
            "WHERE window_start > now() - interval '30 seconds' AND algorithm = 'trustmesh' GROUP BY region").fetchall()
    for region in regions:
        redis_epoch = int(r.get(f"epoch:{region}") or 0)
        check(db_epochs.get(region) == redis_epoch,
              f"{region}: database epoch {db_epochs.get(region)} = Redis epoch {redis_epoch}")

    print("\n4. Trust state shared in Redis (survives restarts and moves with leadership)")
    for region in regions:
        stored = r.hlen(f"trust:{region}")
        expected = sensors_per_region.get(region, 0)
        check(stored >= 0.9 * expected, f"{region}: trust state for {stored} sensors (region has {expected})")

    print("\n5. Standbys compute but do not write")
    expected_rate = cfg.num_sensors / cfg.publish_interval_s
    check(0.8 * expected_rate < float(rate) < 1.3 * expected_rate,
          f"{float(rate):.0f} readings/s stored (expected ~{expected_rate:.0f}; ~{2 * expected_rate:.0f} would mean "
          f"both replicas write)")
    for region, n in sorted(writers):
        check(n == 1, f"{region}: estimates in the last 30 s written by {n} replica(s)")
    fenced = sum(float(x["value"][1]) for x in prom("tm_agg_fenced_writes_total"))
    print(f"  [INFO] fenced writes so far: {fenced:.0f} (non-zero only after a stale-leader experiment)")

    print(f"\n{'All checks passed: Stage 3 complete.' if all(results) else 'Some checks failed.'}")
    print("Next: python -m experiments.failover --mode kill   (also: --mode stop, --mode pause)\n")


if __name__ == "__main__":
    main()
