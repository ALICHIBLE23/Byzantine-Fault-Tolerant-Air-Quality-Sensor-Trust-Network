"""Alerting service: turns the replicas' votes into pollution alerts by quorum.

Subscribes to votes/# (one vote per replica per region per window), decides each window with
services/alerting/book.py, and stores every decision and every alert in PostgreSQL. Open alerts are
reloaded at startup, so a restart neither forgets nor duplicates an alert.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import aiomqtt
import psycopg
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from common.config import load_config
from common.logclient import mqtt_logger, setup_logging
from common.schemas import parse_vote
from services.alerting.book import AlertBook, Event, OpenAlert

VOTES = Counter("tm_alert_votes_total", "Votes received", ["region", "vote"])
INVALID_VOTES = Counter("tm_alert_invalid_votes_total", "Malformed votes")
LATE_VOTES = Gauge("tm_alert_late_votes", "Votes that arrived after their window was decided")
DECISIONS = Counter("tm_alert_decisions_total", "Window decisions", ["region", "decision"])
OPEN = Gauge("tm_alerts_open", "1 while an alert is open for the region", ["region"])
FIRED = Counter("tm_alerts_fired_total", "Alerts opened", ["region"])
DECISION_DELAY = Histogram("tm_alert_decision_delay_seconds", "Window end to decision",
                           buckets=(0.5, 1, 2, 3, 4, 5, 7.5, 10, 15))
DB_CONNECTED = Gauge("tm_alert_db_connected", "1 when connected to the database")

INSERT_DECISION = """
INSERT INTO alert_decisions (region, window_start, decision, votes_over, votes_total, estimate, voters)
VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (region, window_start) DO NOTHING
"""
OPEN_ALERT = """
INSERT INTO alerts (alert_id, region, status, opened_window, peak_estimate, votes_over, votes_total, quorum)
VALUES (%s, %s, 'open', %s, %s, %s, %s, %s) ON CONFLICT (alert_id) DO NOTHING
"""
RAISE_PEAK = "UPDATE alerts SET peak_estimate = GREATEST(peak_estimate, %s) WHERE alert_id = %s"
CLOSE_ALERT = """
UPDATE alerts SET status = 'closed', closed_window = %s, peak_estimate = GREATEST(peak_estimate, %s),
                  closed_at = now() WHERE alert_id = %s
"""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class AlertService:
    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log
        self.book = AlertBook(cfg.alert_quorum, cfg.alert_replicas, cfg.alert_clear_windows, cfg.window_ms,
                              vote_timeout_ms=cfg.window_ms + cfg.grace_ms + 3000)
        self.pending: list[Event] = []
        self.conn: psycopg.AsyncConnection | None = None

    async def _db(self) -> psycopg.AsyncConnection:
        if self.conn is None or self.conn.closed:
            self.conn = await psycopg.AsyncConnection.connect(self.cfg.pg_dsn, autocommit=True)
            DB_CONNECTED.set(1)
        return self.conn

    async def restore(self) -> None:
        for attempt in range(10):
            try:
                conn = await self._db()
                cur = await conn.execute(
                    "SELECT alert_id, region, opened_window, peak_estimate, votes_over, votes_total "
                    "FROM alerts WHERE status = 'open'")
                for alert_id, region, opened, peak, n_over, n_total in await cur.fetchall():
                    self.book.open[region] = OpenAlert(alert_id, region, int(opened.timestamp() * 1000), peak,
                                                       n_over, n_total)
                    OPEN.labels(region).set(1)
                if self.book.open:
                    self.log.info("restored open alerts: %s", sorted(self.book.open))
                return
            except psycopg.Error as exc:
                self.log.warning("cannot load open alerts yet (%s); retry %d/10", exc, attempt + 1)
                await asyncio.sleep(min(2 ** attempt, 10))

    async def consume(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with aiomqtt.Client(self.cfg.mqtt_host, self.cfg.mqtt_port, identifier="alerting",
                                          clean_session=False, logger=mqtt_logger()) as client:
                    await client.subscribe("votes/#", qos=1)
                    backoff = 1.0
                    self.log.info("listening for votes: quorum %d of %d replicas, threshold %.1f ug/m3",
                                  self.cfg.alert_quorum, self.cfg.alert_replicas, self.cfg.alert_threshold)
                    async for message in client.messages:
                        try:
                            vote = parse_vote(message.payload)
                        except ValueError:
                            INVALID_VOTES.inc()
                            continue
                        if self.book.add(vote):
                            VOTES.labels(vote.region, "over" if vote.over else "under").inc()
                        LATE_VOTES.set(self.book.late_votes)
            except aiomqtt.MqttError as exc:
                self.log.warning("broker connection lost (%s); reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def decide_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            now = now_ms()
            for e in self.book.due(now):
                self._observe(e, now)
                self.pending.append(e)
            if self.pending:
                await self._store()

    def _observe(self, e: Event, now: int) -> None:
        if e.kind == "decision":
            DECISIONS.labels(e.region, e.decision).inc()
            DECISION_DELAY.observe(max(0.0, (now - e.window_start - self.cfg.window_ms) / 1000))
            if e.decision == "blocked":
                self.log.warning("alarm BLOCKED for %s: only %d/%d replicas (%s) voted over %.1f, quorum is %d",
                                 e.region, e.votes_over, e.votes_total, ", ".join(e.voters),
                                 self.cfg.alert_threshold, self.cfg.alert_quorum)
        elif e.kind == "open":
            FIRED.labels(e.region).inc()
            OPEN.labels(e.region).set(1)
            self.log.warning("ALERT %s: PM2.5 %.1f ug/m3 above %.1f, confirmed by %d/%d replicas (%s)",
                             e.region, e.estimate, self.cfg.alert_threshold, e.votes_over, e.votes_total,
                             ", ".join(e.voters))
        elif e.kind == "close":
            OPEN.labels(e.region).set(0)
            self.log.info("alert %s closed (peak %.1f ug/m3)", e.alert_id, e.estimate)

    async def _store(self) -> None:
        try:
            conn = await self._db()
            while self.pending:
                e = self.pending[0]
                if e.kind == "decision":
                    await conn.execute(INSERT_DECISION, (e.region, to_dt(e.window_start), e.decision, e.votes_over,
                                                         e.votes_total, e.estimate, ",".join(e.voters)))
                    if e.decision == "alarm" and e.alert_id:
                        await conn.execute(RAISE_PEAK, (e.estimate, e.alert_id))
                elif e.kind == "open":
                    await conn.execute(OPEN_ALERT, (e.alert_id, e.region, to_dt(e.window_start), e.estimate,
                                                    e.votes_over, e.votes_total, self.cfg.alert_quorum))
                else:
                    await conn.execute(CLOSE_ALERT, (to_dt(e.window_start), e.estimate, e.alert_id))
                self.pending.pop(0)               # all writes are idempotent: safe to retry
        except psycopg.Error as exc:
            DB_CONNECTED.set(0)
            self.log.warning("alert store failed (%s); %d events kept for retry", exc, len(self.pending))
            if self.conn is not None:
                await self.conn.close()
            self.conn = None


async def main() -> None:
    cfg = load_config("alerting")
    log = setup_logging(cfg)
    start_http_server(cfg.metrics_port)
    service = AlertService(cfg, log)
    await service.restore()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(service.consume())
        tg.create_task(service.decide_loop())


if __name__ == "__main__":
    asyncio.run(main())
