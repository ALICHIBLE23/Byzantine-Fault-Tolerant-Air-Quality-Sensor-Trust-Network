"""Evaluation observer (NOT part of the trust pipeline).

Publishes the ground truth to Prometheus so Grafana can draw it next to the estimates and show
live error per algorithm. Aggregators never see these values: in a real deployment there is no
ground truth, so this service exists only to evaluate and demonstrate the system.

The truth for a region is the average true value at the region's sensor positions, taken at the
midpoint of the most recently closed window, so it lines up with the tm_agg_estimate gauges.
"""
from __future__ import annotations

import time
from collections import defaultdict

from prometheus_client import Gauge, start_http_server

from common.config import load_config
from common.logclient import setup_logging
from common.truth import truth
from services.sensors.fleet import build_fleet

TRUTH = Gauge("tm_truth", "Ground-truth regional value (evaluation only)", ["region"])
FAULTY = Gauge("tm_truth_faulty_sensors", "Sensors configured as faulty (evaluation only)", ["region"])


def main() -> None:
    cfg = load_config("evaluator")
    log = setup_logging(cfg)
    start_http_server(cfg.metrics_port)
    fleet = build_fleet(cfg)
    positions = defaultdict(list)
    faulty = defaultdict(int)
    for s in fleet:
        positions[s.region].append((s.x_km, s.y_km))
        faulty[s.region] += s.behavior != "honest"
    for region, n in faulty.items():
        FAULTY.labels(region).set(n)
    log.info("evaluator publishing ground truth for %d regions (%d faulty sensors configured)",
             len(positions), sum(faulty.values()))
    window_s, grace_s = cfg.window_ms / 1000, cfg.grace_ms / 1000
    while True:
        horizon = time.time() - grace_s
        closed_up_to = horizon - horizon % window_s
        mid = closed_up_to - window_s / 2
        for region, pts in positions.items():
            TRUTH.labels(region).set(sum(truth(x, y, mid, cfg) for x, y in pts) / len(pts))
        time.sleep(1.0)


if __name__ == "__main__":
    main()
