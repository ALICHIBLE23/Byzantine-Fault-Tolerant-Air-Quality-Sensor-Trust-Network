"""Unit tests for the pure modules (no broker or database needed). Run: pytest"""
import json

import numpy as np
import pytest

from common.aggregation import mean, median, trimmed_mean
from common.config import load_config
from common.geo import all_regions, region_bounds, region_of
from common.schemas import Reading, parse_reading
from common.truth import truth
from services.sensors.fleet import build_fleet

CFG = load_config("test")


def test_truth_is_deterministic_and_non_negative():
    a = truth(3.2, 4.1, 1_700_000_000, CFG)
    b = truth(3.2, 4.1, 1_700_000_000, CFG)
    assert a == b
    assert all(truth(x, y, 1_700_000_000 + t, CFG) >= 0
               for x in range(0, 11, 2) for y in range(0, 11, 2) for t in (0, 300, 600))


def test_truth_varies_in_space_and_time():
    values = {round(truth(x, y, 1_700_000_000, CFG), 3) for x in range(0, 10) for y in range(0, 10)}
    assert len(values) > 50
    assert truth(5, 5, 1_700_000_000, CFG) != truth(5, 5, 1_700_000_450, CFG)


def test_region_mapping_covers_city():
    assert region_of(0, 0, CFG) == "r-0-0"
    assert region_of(CFG.city_w_km - 0.01, CFG.city_h_km - 0.01, CFG) == f"r-{CFG.grid_rows - 1}-{CFG.grid_cols - 1}"
    assert region_of(-5, 999, CFG) in all_regions(CFG)  # out-of-range positions are clamped
    assert len(all_regions(CFG)) == CFG.grid_rows * CFG.grid_cols
    x0, y0, x1, y1 = region_bounds("r-1-0", CFG)
    assert region_of((x0 + x1) / 2, (y0 + y1) / 2, CFG) == "r-1-0"


def test_reading_round_trip():
    r = Reading("s-0001", "r-0-1", 6.5, 1.2, 33.3, 1_700_000_000_000, 42)
    assert parse_reading(r.to_json()) == r


@pytest.mark.parametrize("payload", [
    b"not json",
    b"[1, 2, 3]",
    json.dumps({"sensor_id": "s-1", "region": "r-0-0", "x": 1, "y": 1, "ts": 1, "seq": 1}).encode(),  # no value
    json.dumps({"sensor_id": "s-1", "region": "r-0-0", "x": 1, "y": 1, "value": "abc", "ts": 1, "seq": 1}).encode(),
    json.dumps({"sensor_id": "s-1", "region": "r-0-0", "x": 1, "y": 1, "value": float("nan"), "ts": 1, "seq": 1}).encode(),
    json.dumps({"v": 99, "sensor_id": "s-1", "region": "r-0-0", "x": 1, "y": 1, "value": 1, "ts": 1, "seq": 1}).encode(),
])
def test_invalid_readings_are_rejected(payload):
    with pytest.raises(ValueError):
        parse_reading(payload)


def test_baselines():
    v = np.array([10.0, 11.0, 12.0, 13.0, 1000.0])
    assert mean(v) == pytest.approx(209.2)
    assert median(v) == 12.0
    assert trimmed_mean(v) == pytest.approx(12.0)  # the outlier is trimmed away


def test_fleet_is_deterministic_and_inside_city():
    a, b = build_fleet(CFG), build_fleet(CFG)
    assert a == b
    assert len(a) == CFG.num_sensors
    assert len({s.sensor_id for s in a}) == len(a)
    assert all(0 <= s.x_km < CFG.city_w_km and 0 <= s.y_km < CFG.city_h_km for s in a)
    assert {s.region for s in a} == set(all_regions(CFG))
