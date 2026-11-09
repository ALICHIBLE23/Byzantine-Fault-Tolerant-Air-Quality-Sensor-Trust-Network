"""Quorum-based alert decisions (pure logic, no I/O, so it can be unit-tested).

Every aggregator replica of a region votes once per window: is the region above the threshold?
A window is decided when all expected replicas have voted, or when the vote timeout expires:

  alarm    at least `quorum` replicas voted "over"
  blocked  some replicas voted "over", but fewer than `quorum` (a lone faulty or lagging replica
           cannot raise an alert on its own)
  clear    no replica voted "over"

Alerts follow the decisions with hysteresis: an alert OPENS on the first alarm window and CLOSES
after `clear_windows` consecutive non-alarm windows, so one noisy window does not flap it.
Windows of a region are decided strictly in order. Alert ids are deterministic (region@window), so
replaying the same votes gives the same alerts (idempotent writes).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from common.schemas import Vote


@dataclass
class OpenAlert:
    alert_id: str
    region: str
    opened_window: int
    peak: float
    votes_over: int
    votes_total: int


@dataclass
class Event:
    kind: str                 # "decision" | "open" | "close"
    region: str
    window_start: int
    decision: str = ""        # for decisions: alarm | blocked | clear
    votes_over: int = 0
    votes_total: int = 0
    estimate: float = 0.0
    alert_id: str = ""
    voters: list[str] = field(default_factory=list)


class AlertBook:
    def __init__(self, quorum: int, replicas: int, clear_windows: int, window_ms: int, vote_timeout_ms: int):
        if not 1 <= quorum <= replicas:
            raise ValueError(f"quorum must be between 1 and {replicas}, got {quorum}")
        self.quorum = quorum
        self.replicas = replicas
        self.clear_windows = clear_windows
        self.window_ms = window_ms
        self.vote_timeout_ms = vote_timeout_ms
        self.votes: dict[tuple[str, int], dict[str, Vote]] = {}
        self.decided_up_to: dict[str, int] = {}     # windows of a region below this are decided
        self.open: dict[str, OpenAlert] = {}
        self.clear_streak: dict[str, int] = {}
        self.late_votes = 0

    def add(self, vote: Vote) -> bool:
        """Record a vote. Returns False for duplicates and votes for windows already decided."""
        if vote.window_start < self.decided_up_to.get(vote.region, -1):
            self.late_votes += 1
            return False
        ballots = self.votes.setdefault((vote.region, vote.window_start), {})
        if vote.replica in ballots:
            return False
        ballots[vote.replica] = vote
        return True

    def due(self, now_ms: int) -> list[Event]:
        events: list[Event] = []
        for region in sorted({r for r, _ in self.votes}):
            for ws in sorted(w for r, w in self.votes if r == region):
                ballots = self.votes[(region, ws)]
                complete = len(ballots) >= self.replicas
                expired = now_ms >= ws + self.vote_timeout_ms
                if not (complete or expired):
                    break                               # decide windows strictly in order
                events += self._decide(region, ws, ballots)
                del self.votes[(region, ws)]
                self.decided_up_to[region] = ws + self.window_ms
        return events

    def _decide(self, region: str, ws: int, ballots: dict[str, Vote]) -> list[Event]:
        over = [v for v in ballots.values() if v.over]
        n_over, n_total = len(over), len(ballots)
        # Reported level: the highest value that a full quorum supports (the quorum-th highest "over"
        # estimate), so a minority of lying replicas cannot inflate the alert's magnitude either.
        over_values = sorted((v.estimate for v in over), reverse=True)
        if n_over >= self.quorum:
            estimate = over_values[self.quorum - 1]
        else:
            estimate = sorted(v.estimate for v in ballots.values())[(n_total - 1) // 2]   # median opinion
        decision = "alarm" if n_over >= self.quorum else ("blocked" if n_over else "clear")
        events = [Event("decision", region, ws, decision, n_over, n_total, estimate,
                        voters=sorted(v.replica for v in over))]
        alert = self.open.get(region)
        if decision == "alarm":
            self.clear_streak[region] = 0
            if alert is None:
                alert = OpenAlert(f"{region}@{ws}", region, ws, estimate, n_over, n_total)
                self.open[region] = alert
                events.append(Event("open", region, ws, decision, n_over, n_total, estimate, alert.alert_id,
                                    events[0].voters))
            else:
                alert.peak = max(alert.peak, estimate)
            events[0].alert_id = alert.alert_id
        elif alert is not None:
            self.clear_streak[region] = self.clear_streak.get(region, 0) + 1
            if self.clear_streak[region] >= self.clear_windows:
                del self.open[region]
                events.append(Event("close", region, ws, decision, n_over, n_total, alert.peak, alert.alert_id))
        return events
