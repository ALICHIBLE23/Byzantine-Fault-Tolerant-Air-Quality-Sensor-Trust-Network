"""Concurrency control on shared border reputations (against an in-memory Redis stand-in)."""
import asyncio
import random

import pytest

from common.trust import TrustParams, update_reputation
from services.aggregator.reputation import STRATEGIES, SharedReputation, rep_key
from tests.fake_redis import FakeRedis


def run(coro):
    return asyncio.run(coro)


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_single_writer_applies_the_update_rule(strategy):
    async def go():
        store = SharedReputation(FakeRedis(), strategy)
        r1, applied1, _, _ = await store.apply("s-1", 0.2, "r-0-0", 100, prior=0.9)
        r2, applied2, _, _ = await store.apply("s-1", 1.0, "r-1-1", 100, prior=0.9)
        return r1, applied1, r2, applied2
    r1, a1, r2, a2 = run(go())
    p = TrustParams()
    assert a1 and a2
    assert r1 == pytest.approx(update_reputation(0.9, 0.2, p), abs=1e-9)
    assert r2 == pytest.approx(update_reputation(r1, 1.0, p), abs=1e-9)   # second region builds on the first


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_the_same_window_is_applied_once(strategy):
    async def go():
        store = SharedReputation(FakeRedis(), strategy)
        first = await store.apply("s-1", 0.2, "r-0-0", 100, prior=0.9)
        again = await store.apply("s-1", 0.2, "r-0-0", 100, prior=0.9)   # e.g. a retry after failover
        return first, again
    first, again = run(go())
    assert first[1] and not again[1]
    assert again[0] == pytest.approx(first[0])


def hammer(strategy, workers=8, updates=40, keys=2, think_ms=1.0):
    """Many concurrent writers on few keys; returns (lost updates, total retries)."""
    async def go():
        redis = FakeRedis()
        store = SharedReputation(redis, strategy, think_ms=think_ms)
        retries = 0

        async def worker(w):
            nonlocal retries
            rng = random.Random(w)
            for i in range(updates):
                _, _, r, _ = await store.apply(f"s-{rng.randrange(keys)}", rng.random(), f"w{w}", i, 0.5)
                retries += r
        await asyncio.gather(*(worker(w) for w in range(workers)))
        applied = sum(int(redis.hashes.get(rep_key(f"s-{k}"), {}).get("n", 0)) for k in range(keys))
        return workers * updates - applied, retries
    return run(go())


def test_no_control_loses_updates_under_contention():
    lost, _ = hammer("none")
    assert lost > 0


@pytest.mark.parametrize("strategy", ["optimistic", "lock", "atomic"])
def test_controlled_strategies_lose_nothing(strategy):
    lost, retries = hammer(strategy)
    assert lost == 0
    if strategy == "optimistic":
        assert retries > 0          # conflicts were detected and retried instead of lost
