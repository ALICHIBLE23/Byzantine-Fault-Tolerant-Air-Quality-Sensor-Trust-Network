"""Unit tests for the trust algorithm on a synthetic, smoothly varying field."""
import random

import numpy as np
import pytest

from common.trust import TrustEngine, TrustParams, weighted_median


def field(x, y, w):
    """Smooth field with a gentle gradient that changes slowly over windows."""
    return 25 + 2 * x + 1.5 * np.sin(y) + 0.3 * np.sin(w / 5)


@pytest.fixture
def grid():
    rng = random.Random(1)
    pts = [(rng.uniform(0, 4), rng.uniform(0, 4)) for _ in range(120)]
    ids = [f"s-{i}" for i in range(len(pts))]
    xs = np.array([p[0] for p in pts])
    ys = np.array([p[1] for p in pts])
    return ids, xs, ys, rng


def run(engine, ids, xs, ys, rng, windows, liars=(), offset=25.0, stuck=(), start=0):
    frozen = {}
    result = None
    for w in range(start, start + windows):
        values = field(xs, ys, w) + np.array([rng.gauss(0, 1.0) for _ in ids])
        for i in liars:
            values[i] += offset
        for i in stuck:
            frozen.setdefault(i, round(float(values[i]), 3))
            values[i] = frozen[i]
        result = engine.process_window(ids, xs, ys, values)
    return result


def test_weighted_median():
    assert weighted_median(np.array([1.0, 2.0, 3.0]), np.array([1.0, 1.0, 1.0])) == 2.0
    assert weighted_median(np.array([1.0, 2.0, 100.0]), np.array([1.0, 1.0, 10.0])) == 100.0


def test_honest_sensors_stay_trusted_and_estimate_is_unbiased(grid):
    ids, xs, ys, rng = grid
    engine = TrustEngine()
    result = run(engine, ids, xs, ys, rng, 40)
    assert result.n_flagged <= 1
    assert result.estimate == pytest.approx(float(np.mean(field(xs, ys, 39))), abs=0.6)
    assert min(st.reputation for st in engine.states.values()) > 0.4


def test_liars_are_flagged_quickly_and_excluded(grid):
    ids, xs, ys, rng = grid
    liars = list(range(0, 120, 6))  # 20 of 120 sensors, about 17%
    engine = TrustEngine()
    result = run(engine, ids, xs, ys, rng, 8, liars=liars)
    flagged = {v.sensor_id for v in result.verdicts if v.flagged}
    assert {ids[i] for i in liars} <= flagged
    assert len(flagged - {ids[i] for i in liars}) <= 2
    true_mean = float(np.mean(field(xs, ys, 7)))
    assert abs(result.estimate - true_mean) < 1.0
    assert abs(float(np.mean([v.value for v in result.verdicts])) - true_mean) > 3.0  # the plain mean is fooled


def test_stuck_sensor_is_detected(grid):
    ids, xs, ys, rng = grid
    engine = TrustEngine(TrustParams(stuck_neighbor_move=0.0))
    result = run(engine, ids, xs, ys, rng, 15, stuck=[5])
    verdict = next(v for v in result.verdicts if v.sensor_id == ids[5])
    assert verdict.flagged


def test_trust_is_lost_faster_than_regained(grid):
    """On-off attacker: builds reputation, lies briefly, then needs far longer to earn it back."""
    ids, xs, ys, rng = grid
    engine = TrustEngine()
    run(engine, ids, xs, ys, rng, 40)
    before = engine.states[ids[10]].reputation
    lying = 0
    while not engine.states[ids[10]].flagged:
        run(engine, ids, xs, ys, rng, 1, liars=[10], start=40 + lying)
        lying += 1
    recovering = 0
    while engine.states[ids[10]].reputation < before - 0.05 and recovering < 200:
        run(engine, ids, xs, ys, rng, 1, start=40 + lying + recovering)
        recovering += 1
    assert before > 0.7
    assert lying <= 4                 # flagged within a few windows of lying
    assert recovering > 5 * lying     # regaining the old reputation takes much longer


def test_empty_window():
    result = TrustEngine().process_window([], [], [], [])
    assert result.n_sensors == 0
