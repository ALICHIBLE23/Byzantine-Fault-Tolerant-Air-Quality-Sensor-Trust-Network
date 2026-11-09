"""Leader election, failover and fencing, with a controllable clock (no Redis needed)."""
import asyncio
import logging

import pytest

from common.config import load_config
from common.schemas import Reading
from common.trust import TrustEngine
from services.aggregator import main as agg_module
from services.aggregator.leases import InMemoryLeaseBackend, LeaseManager
from services.aggregator.main import Aggregator

REGIONS = ["r-0-0", "r-0-1"]


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def manager(backend, owner, clock, regions=REGIONS, self_check=True):
    return LeaseManager(backend, regions, owner, ttl_ms=3000, renew_interval_s=1.0,
                        self_check=self_check, clock=clock)


def run(coro):
    return asyncio.run(coro)


def test_first_replica_leads_all_regions_with_epoch_1():
    clock = Clock()
    a = manager(InMemoryLeaseBackend(clock), "a", clock)
    run(a.tick())
    assert all(a.is_leader(r) for r in REGIONS)
    assert all(a.epoch(r) == 1 for r in REGIONS)


def test_only_one_leader_per_region():
    clock = Clock()
    backend = InMemoryLeaseBackend(clock)
    a, b = manager(backend, "a", clock), manager(backend, "b", clock)
    for _ in range(5):
        run(a.tick())
        run(b.tick())
        clock.t += 1
        for r in REGIONS:
            assert a.is_leader(r) != b.is_leader(r)


def test_crashed_leader_is_replaced_after_ttl_with_newer_epoch():
    clock = Clock()
    backend = InMemoryLeaseBackend(clock)
    a, b = manager(backend, "a", clock), manager(backend, "b", clock)
    run(a.tick()); run(b.tick())
    assert a.is_leader("r-0-0") and not b.is_leader("r-0-0")
    # a crashes (stops renewing); b keeps trying every second
    takeover = None
    for second in range(1, 10):
        clock.t += 1
        run(b.tick())
        if b.is_leader("r-0-0"):
            takeover = second
            break
    assert takeover is not None and takeover <= 4      # within TTL (3 s) + one renew interval
    assert b.epoch("r-0-0") == 2


def test_paused_leader_stops_acting_before_it_learns_and_steps_down_on_resume():
    clock = Clock()
    backend = InMemoryLeaseBackend(clock)
    a, b = manager(backend, "a", clock), manager(backend, "b", clock)
    run(a.tick()); run(b.tick())
    clock.t += 2.5                       # a is frozen: no renewals
    assert not a.is_leader("r-0-0")      # self-check: 2.5 s > 80% of the 3 s TTL
    clock.t += 2.0
    run(b.tick())
    assert b.is_leader("r-0-0")
    run(a.tick())                        # a wakes up: renewal fails, it steps down
    assert "r-0-0" not in a.held


def test_without_self_check_a_paused_leader_still_believes_it_leads():
    """This is what the database fencing is for (FENCING_SELF_CHECK=false experiment)."""
    clock = Clock()
    backend = InMemoryLeaseBackend(clock)
    a = manager(backend, "a", clock, self_check=False)
    b = manager(backend, "b", clock)
    run(a.tick()); run(b.tick())
    clock.t += 5
    run(b.tick())
    assert b.is_leader("r-0-0") and a.is_leader("r-0-0")   # two "leaders": epochs 2 vs 1
    assert b.epoch("r-0-0") > a.epoch("r-0-0")


def test_graceful_release_allows_immediate_takeover():
    clock = Clock()
    backend = InMemoryLeaseBackend(clock)
    a, b = manager(backend, "a", clock), manager(backend, "b", clock)
    run(a.tick()); run(b.tick())
    run(a.release_all())
    run(b.tick())                         # no TTL wait
    assert all(b.is_leader(r) for r in REGIONS)


def test_lease_store_outage_demotes_the_leader_after_ttl():
    clock = Clock()
    backend = InMemoryLeaseBackend(clock)
    a = manager(backend, "a", clock)
    run(a.tick())

    async def broken(*args, **kwargs):
        raise ConnectionError("redis down")
    backend.renew = broken
    clock.t += 2
    run(a.tick())
    assert "r-0-0" in a.held              # still within the TTL
    clock.t += 2
    run(a.tick())
    assert not a.held                     # cannot prove leadership: steps down (consistency over availability)


def test_trust_state_round_trip():
    engine = TrustEngine()
    engine.process_window(["s-1", "s-2"], [0.0, 0.1], [0.0, 0.1], [10.0, 11.0])
    copy = TrustEngine()
    copy.import_state(engine.export_state())
    assert copy.export_state() == engine.export_state()


def test_failover_hands_over_standby_windows(monkeypatch):
    """Two replicas get the same stream; the leader dies; the standby's backlog fills the gap."""
    clock = Clock()
    ms = {"now": 1_700_000_000_000}
    monkeypatch.setattr(agg_module, "now_ms", lambda: ms["now"])
    backend = InMemoryLeaseBackend(clock)
    cfg = load_config("test")
    log = logging.getLogger("test")
    la, lb = manager(backend, "a", clock, REGIONS[:1]), manager(backend, "b", clock, REGIONS[:1])
    a = Aggregator(cfg, log, la)
    b = Aggregator(cfg, log, lb)
    la.on_promote, la.on_demote = a.on_promote, a.on_demote
    lb.on_promote, lb.on_demote = b.on_promote, b.on_demote
    run(la.tick()); run(lb.tick())
    assert la.is_leader("r-0-0") and not lb.is_leader("r-0-0")

    ws = a.accept_from
    for w in range(2):
        for i in range(5):
            payload = Reading(f"s-{i}", "r-0-0", 0.1 * i, 0.1, 20.0, ws + w * 5000 + 10, w + 1).to_json()
            a.handle(payload, ms["now"])
            b.handle(payload, ms["now"])
    ms["now"] = ws + 3 * 5000
    run(a.close_due_windows())
    run(b.close_due_windows())
    assert len(a.estimate_rows) == 2 * 4          # leader buffered 2 windows x 4 algorithms
    assert b.estimate_rows == [] and len(b.backlog["r-0-0"]) == 2

    clock.t += 5                                  # leader a is dead; b takes over
    run(lb.tick())
    assert lb.is_leader("r-0-0") and lb.epoch("r-0-0") == 2
    handed = {(row[1], row[3]) for row, _ in b.estimate_rows}
    assert len(handed) == 2 * 4                   # both windows recovered from the standby
    assert all(row[-1] == 2 for row, _ in b.estimate_rows)   # re-tagged with the new epoch
    assert b.epochs_to_publish == {"r-0-0": 2}    # published before the first write
