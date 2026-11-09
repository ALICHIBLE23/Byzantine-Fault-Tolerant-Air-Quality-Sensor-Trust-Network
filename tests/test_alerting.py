"""Quorum alert decisions."""
import pytest

from common.schemas import Vote, parse_vote
from services.alerting.book import AlertBook

W = 5000


def book(quorum=2, replicas=2, clear=2):
    return AlertBook(quorum, replicas, clear, W, vote_timeout_ms=W + 5000)


def vote(replica, over, ws=0, region="r-1-1", estimate=None):
    return Vote(region, ws, replica, 1, replica.endswith("1"), estimate if estimate is not None else (40.0 if over else 30.0), over)


def kinds(events):
    return [(e.kind, e.decision) for e in events]


def test_vote_round_trip():
    v = vote("b1", True, ws=123)
    assert parse_vote(v.to_json()) == v
    with pytest.raises(ValueError):
        parse_vote(b'{"region": "r-1-1"}')


def test_alarm_needs_quorum_and_opens_one_alert():
    b = book()
    b.add(vote("b1", True))
    assert b.due(1) == []                            # waiting for the second replica
    b.add(vote("b2", True))
    events = b.due(2)
    assert kinds(events) == [("decision", "alarm"), ("open", "alarm")]
    assert events[1].alert_id == "r-1-1@0"
    b.add(vote("b1", True, ws=W)); b.add(vote("b2", True, ws=W))
    assert kinds(b.due(W + 1)) == [("decision", "alarm")]   # still the same alert, no second "open"


def test_a_single_lying_replica_is_blocked():
    b = book()
    b.add(vote("b1", False)); b.add(vote("b2", True, estimate=70.0))
    events = b.due(1)
    assert kinds(events) == [("decision", "blocked")]
    assert events[0].voters == ["b2"] and not b.open


def test_quorum_one_lets_a_single_replica_raise_the_alarm():
    b = book(quorum=1)
    b.add(vote("b1", False)); b.add(vote("b2", True))
    assert ("open", "alarm") in kinds(b.due(1))


def test_missing_replica_decided_at_timeout_and_cannot_alarm_alone():
    b = book()
    b.add(vote("b1", True))                          # b2 is down
    assert b.due(W) == []
    assert kinds(b.due(W + 5000)) == [("decision", "blocked")]   # availability lost: consistency kept


def test_alert_closes_after_consecutive_clear_windows():
    b = book(clear=2)
    for ws, over in [(0, True), (W, False), (2 * W, True), (3 * W, False), (4 * W, False)]:
        b.add(vote("b1", over, ws)); b.add(vote("b2", over, ws))
    events = b.due(10 * W)
    assert [e.kind for e in events if e.kind != "decision"] == ["open", "close"]   # one dip does not close it
    assert next(e for e in events if e.kind == "close").window_start == 4 * W


def test_duplicate_and_late_votes_are_ignored():
    b = book()
    assert b.add(vote("b1", True))
    assert not b.add(vote("b1", True))
    b.add(vote("b2", True)); b.due(1)
    assert not b.add(vote("b1", False))              # window already decided
    assert b.late_votes == 1


def test_windows_are_decided_in_order():
    b = book()
    b.add(vote("b1", True, ws=W)); b.add(vote("b2", True, ws=W))   # complete
    b.add(vote("b1", True, ws=0))                                   # earlier window still incomplete
    assert b.due(W + 1) == []
    events = b.due(W + 5000)                                        # ws=0 times out, then ws=W
    assert [e.window_start for e in events if e.kind == "decision"] == [0, W]


def test_a_lying_replica_cannot_inflate_the_reported_level():
    b = book()
    b.add(vote("b1", True, estimate=41.0)); b.add(vote("b2", True, estimate=71.0))   # b2 adds +30
    opened = next(e for e in b.due(1) if e.kind == "open")
    assert opened.estimate == 41.0
