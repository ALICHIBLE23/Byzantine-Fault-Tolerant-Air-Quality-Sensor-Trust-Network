"""Stage 4 check: border exchange and shared border reputations.

Run from the repo root after the stack has run for a few minutes (with a BEHAVIOR_MIX set):
    python -m experiments.check_stage4
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
from common.geo import distance_to_own_edge, regions_within  # noqa: E402
from common.trust import TrustParams  # noqa: E402

PROM = f"http://localhost:{os.getenv('PROMETHEUS_PORT', '9090')}"
EDGE_KM = 0.3
results: list[bool] = []


def check(passed: bool, message: str) -> None:
    results.append(passed)
    print(f"  [{'OK' if passed else 'FAIL':4}] {message}")


def prom(query: str) -> list[dict]:
    with urllib.request.urlopen(f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": query}), timeout=3) as resp:
        return json.load(resp)["data"]["result"]


def main() -> None:
    cfg = load_config("check-stage4")
    radius = TrustParams().radius_km
    r = redis.Redis(host="localhost", port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=True)
    with psycopg.connect(cfg.pg_dsn) as conn:
        sensors = conn.execute("SELECT sensor_id, region, x_km, y_km, behavior FROM sensors").fetchall()
        flagged = {sid for sid, f in conn.execute("SELECT sensor_id, flagged FROM sensor_trust").fetchall() if f}

    print("\n1. Border readings flowing between partitions")
    per_replica = {x["metric"]["instance"].split(":")[0]: float(x["value"][1])
                   for x in prom("sum by (instance) (rate(tm_agg_border_readings_total[1m]))")}
    for replica in ["aggregator-a1", "aggregator-a2", "aggregator-b1", "aggregator-b2"]:
        rate = per_replica.get(replica, 0.0)
        check(rate > 0, f"{replica}: {rate:.1f} border readings/s from the other partition")

    print("\n2. Shared reputations of border sensors")
    border = {sid: regions_within(x, y, cfg, radius) for sid, _, x, y, _ in sensors}
    border = {sid: n for sid, n in border.items() if n}
    stored = {sid for sid in border if r.exists(f"rep:{sid}")}
    check(len(stored) >= 0.9 * len(border),
          f"{len(stored)}/{len(border)} border sensors have a shared reputation record in Redis")
    multi = 0
    for sid in list(stored)[:200]:
        fields = r.hgetall(f"rep:{sid}")
        regions_seen = {k[2:] for k in fields if k.startswith("w:")}
        multi += len(regions_seen) >= 2
    sample = min(200, len(stored))
    check(multi >= 0.8 * sample, f"{multi}/{sample} sampled records hold evidence from 2+ regions "
                                 f"(the sensor is judged from both sides of the boundary)")
    example = next(iter(stored), None)
    if example:
        print(f"     example rep:{example} = {r.hgetall(f'rep:{example}')}")

    print("\n3. Concurrency control")
    strategy = [x["metric"]["strategy"] for x in prom("sum by (strategy) (tm_rep_updates_total)")]
    conflicts = sum(float(x["value"][1]) for x in prom("tm_rep_conflicts_total"))
    updates = sum(float(x["value"][1]) for x in prom('tm_rep_updates_total{result="applied"}'))
    print(f"  [INFO] strategy in use: {', '.join(strategy) or 'n/a'}; {updates:.0f} updates applied, "
          f"{conflicts:.0f} optimistic conflicts caught (each would have been a lost update)")
    errors = sum(float(x["value"][1]) for x in prom("tm_agg_shared_reputation_errors_total"))
    check(errors == 0, f"{errors:.0f} failed shared reputation reads/updates")

    print(f"\n4. Accuracy at region edges (sensors < {EDGE_KM} km from their region boundary)")
    buckets = defaultdict(lambda: [0, 0, []])
    for sid, region, x, y, behavior in sensors:
        bucket = "edge" if distance_to_own_edge(x, y, cfg) < EDGE_KM else "interior"
        wrong = (sid in flagged) != (behavior != "honest")
        buckets[bucket][0] += wrong
        buckets[bucket][1] += 1
        if wrong:
            buckets[bucket][2].append(f"{sid} ({behavior}, {'flagged' if sid in flagged else 'missed'})")
    for bucket in ("edge", "interior"):
        wrong, total, names = buckets[bucket]
        print(f"  [INFO] misjudged {bucket} sensors: {wrong}/{total} ({wrong / max(total, 1):.1%})"
              + (f": {', '.join(names[:6])}" if names else ""))
    edge_rate = buckets["edge"][0] / max(buckets["edge"][1], 1)
    check(edge_rate <= 0.03, f"edge error rate {edge_rate:.1%} (Stage 2 measured ~5% before border exchange)")

    print(f"\n{'All checks passed: Stage 4 complete.' if all(results) else 'Some checks failed.'}")
    print("Next: python -m experiments.concurrency_bench\n")


if __name__ == "__main__":
    main()
