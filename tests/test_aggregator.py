"""Aggregator logic tests: deduplication, windowing, late readings, invalid input.
Needs the service libraries installed (pip install -r requirements.txt), but no broker or database."""
import asyncio
import dataclasses
import logging

import pytest

from common.config import load_config
from common.schemas import Reading
from services.aggregator import main as agg_module
from services.aggregator.leases import StaticLeases
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
    agg = Aggregator(load_config("test"), logging.getLogger("test"), StaticLeases())
    return agg, clock


def reading(sensor, seq, ts, value=20.0, region="r-0-0"):
    return Reading(sensor, region, 1.0, 1.0, value, ts, seq).to_json()


def test_window_is_closed_after_grace_and_produces_all_algorithms(setup):
    agg, clock = setup
    ws = agg.accept_from
    for i in range(5):
        assert agg.handle(reading(f"s-{i}", 1, ws + 100, value=10.0 + i), clock()) == "ok"
    clock.ms = ws + agg.window_ms + agg.cfg.grace_ms - 1
    asyncio.run(agg.close_due_windows())
    assert agg.estimate_rows == []  # still inside the grace period
    clock.ms += 2
    asyncio.run(agg.close_due_windows())
    rows = {row[3]: row for row, _ in agg.estimate_rows}
    assert set(rows) == {"mean", "median", "trimmed_mean", "trustmesh"}
    assert rows["mean"][4] == pytest.approx(12.0)
    assert rows["mean"][6] == 5  # n_sensors


def test_each_sensor_counts_once_per_window(setup):
    agg, clock = setup
    ws = agg.accept_from
    agg.handle(reading("s-a", 1, ws + 10, value=10.0), clock())
    agg.handle(reading("s-a", 2, ws + 2010, value=30.0), clock())  # same sensor, averaged to 20
    agg.handle(reading("s-b", 1, ws + 10, value=40.0), clock())
    clock.ms = ws + 60_000
    asyncio.run(agg.close_due_windows())
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
    asyncio.run(agg.close_due_windows())
    assert agg.handle(reading("s-1", 1, ws + 10), clock()) == "late"
    assert len(agg.reading_rows) == 1
    assert not agg.windows


def test_window_close_records_trust_for_every_sensor(setup):
    agg, clock = setup
    ws = agg.accept_from
    for i in range(6):
        agg.handle(Reading(f"s-{i}", "r-0-0", 0.1 * i, 0.1, 20.0 + i * 0.1, ws + 50, 1).to_json(), clock())
    clock.ms = ws + 60_000
    asyncio.run(agg.close_due_windows())
    assert len(agg.trust_rows) == 6 and len(agg.history_rows) == 6
    assert "r-0-0" in agg.engines


def test_invalid_and_future_readings_are_rejected(setup):
    agg, clock = setup
    assert agg.handle(b"{broken", clock()) == "invalid"
    assert agg.handle(reading("s-1", 1, clock() + 3_600_000), clock()) == "invalid"
    assert agg.reading_rows == []


def make_agg(monkeypatch, regions):
    clock = FakeClock(1_700_000_000_000)
    monkeypatch.setattr(agg_module, "now_ms", clock)
    cfg = dataclasses.replace(load_config("test"), regions=regions)
    return Aggregator(cfg, logging.getLogger("test"), StaticLeases()), clock


def test_border_exchange_subscribes_only_to_border_cells(monkeypatch):
    agg, _ = make_agg(monkeypatch, "r-0-0")
    topics = agg.topics()
    assert "readings/r-0-0/#" in topics
    assert "readings/r-0-1/c-0-5/#" in topics          # first column of cells across the east edge
    assert not any(t.startswith("readings/r-0-1/c-0-6") for t in topics)   # 2 km away: not needed
    assert len(topics) == 1 + 11


def test_foreign_border_sensor_is_context_not_owned(monkeypatch):
    agg, clock = make_agg(monkeypatch, "r-0-0")
    ws = agg.accept_from
    for i in range(5):
        agg.handle(Reading(f"s-{i}", "r-0-0", 4.6 + 0.05 * i, 2.0, 20.0, ws + 10, 1).to_json(), clock())
    near = Reading("s-near", "r-0-1", 5.3, 2.0, 21.0, ws + 10, 1).to_json()    # 0.3 km across the edge
    far = Reading("s-far", "r-0-1", 8.0, 2.0, 99.0, ws + 10, 1).to_json()      # 3 km away
    assert agg.handle(near, clock()) == "border"
    assert agg.handle(far, clock()) == "ignored"
    assert len(agg.reading_rows) == 5                    # the owner stores foreign readings, not us
    clock.ms = ws + 60_000
    asyncio.run(agg.close_due_windows())
    mean_row = next(row for row, _ in agg.estimate_rows if row[3] == "mean")
    assert mean_row[4] == pytest.approx(20.0)            # context sensor not in the estimate
    assert mean_row[6] == 5
    assert {r[0] for r in agg.trust_rows} == {f"s-{i}" for i in range(5)}
    assert agg.is_border["s-near"] and agg.is_border["s-4"] and not agg.is_border.get("s-far", False)


def test_every_window_produces_a_vote(setup):
    agg, clock = setup
    ws = agg.accept_from
    for i in range(5):
        agg.handle(Reading(f"s-{i}", "r-0-0", 0.1 * i, 0.1, 50.0, ws + 10, 1).to_json(), clock())
    clock.ms = ws + 60_000
    asyncio.run(agg.close_due_windows())
    vote = agg.vote_queue.get_nowait()
    assert vote.region == "r-0-0" and vote.window_start == ws
    assert vote.over and vote.estimate == pytest.approx(50.0, abs=0.5)
