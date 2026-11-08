"""Aggregator logic tests: deduplication, windowing, late readings, invalid input.
Needs the service libraries installed (pip install -r requirements.txt), but no broker or database."""
import logging

import pytest

from common.config import load_config
from common.schemas import Reading
from services.aggregator import main as agg_module
from services.aggregator.main import Aggregator


class FakeClock:
    def __init__(self, ms):
        self.ms = ms

    def __call__(self):
        return self.ms


@pytest.fixture
def setup(monkeypatch):
    clock = FakeClock(1_700_000_000_000)
    monkeypatch.setattr(agg_module, "now_ms", clock)
    agg = Aggregator(load_config("test"), logging.getLogger("test"))
    return agg, clock


def reading(sensor, seq, ts, value=20.0, region="r-0-0"):
    return Reading(sensor, region, 1.0, 1.0, value, ts, seq).to_json()


def test_window_is_closed_after_grace_and_produces_all_algorithms(setup):
    agg, clock = setup
    ws = agg.accept_from
    for i in range(5):
        assert agg.handle(reading(f"s-{i}", 1, ws + 100, value=10.0 + i), clock()) == "ok"
    clock.ms = ws + agg.window_ms + agg.cfg.grace_ms - 1
    agg.close_due_windows()
    assert agg.estimate_rows == []  # still inside the grace period
    clock.ms += 2
    agg.close_due_windows()
    rows = {row[3]: row for row, _ in agg.estimate_rows}
    assert set(rows) == {"mean", "median", "trimmed_mean"}
    assert rows["mean"][4] == pytest.approx(12.0)
    assert rows["mean"][6] == 5  # n_sensors


def test_each_sensor_counts_once_per_window(setup):
    agg, clock = setup
    ws = agg.accept_from
    agg.handle(reading("s-a", 1, ws + 10, value=10.0), clock())
    agg.handle(reading("s-a", 2, ws + 2010, value=30.0), clock())  # same sensor, averaged to 20
    agg.handle(reading("s-b", 1, ws + 10, value=40.0), clock())
    clock.ms = ws + 60_000
    agg.close_due_windows()
    mean_row = next(row for row, _ in agg.estimate_rows if row[3] == "mean")
    assert mean_row[4] == pytest.approx(30.0)  # (20 + 40) / 2, not (10 + 30 + 40) / 3
    assert mean_row[5] == 3 and mean_row[6] == 2


def test_duplicates_are_dropped(setup):
    agg, clock = setup
    ts = agg.accept_from + 5
    assert agg.handle(reading("s-1", 7, ts), clock()) == "ok"
    assert agg.handle(reading("s-1", 7, ts), clock()) == "duplicate"
    assert len(agg.reading_rows) == 1


def test_late_readings_are_stored_but_not_aggregated(setup):
    agg, clock = setup
    ws = agg.accept_from
    clock.ms = ws + agg.window_ms + agg.cfg.grace_ms + 1
    agg.close_due_windows()
    assert agg.handle(reading("s-1", 1, ws + 10), clock()) == "late"
    assert len(agg.reading_rows) == 1
    assert not agg.windows


def test_invalid_and_future_readings_are_rejected(setup):
    agg, clock = setup
    assert agg.handle(b"{broken", clock()) == "invalid"
    assert agg.handle(reading("s-1", 1, clock() + 3_600_000), clock()) == "invalid"
    assert agg.reading_rows == []
