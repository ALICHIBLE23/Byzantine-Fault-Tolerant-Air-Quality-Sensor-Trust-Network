"""Regional aggregator.

Stage 1: a single instance that handles all regions (REGIONS=all) and computes the baseline
estimates (mean, median, trimmed mean) per region per time window.
Later stages add the trust algorithm, replicas with a leader lease, and concurrency control.

Three independent loops run concurrently, each with its own error handling and reconnection:
  consume  - MQTT -> validate -> deduplicate -> place into time windows
  close    - when a window's grace period has passed, compute the estimates
  write    - batch readings and estimates into PostgreSQL
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import aiomqtt
import numpy as np
import psycopg
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from common.aggregation import ALGORITHMS
from common.config import Config, load_config
from common.logclient import mqtt_logger, setup_logging
from common.schemas import DEADLETTER_TOPIC, parse_reading

READINGS = Counter("tm_agg_readings_total", "Valid, unique readings received", ["region"])
DUPLICATES = Counter("tm_agg_duplicates_total", "Duplicate readings dropped")
LATE = Counter("tm_agg_late_total", "Readings that arrived after their window closed")
INVALID = Counter("tm_agg_invalid_total", "Malformed readings sent to the dead-letter topic")
WINDOWS_CLOSED = Counter("tm_agg_windows_closed_total", "Windows aggregated", ["region"])
ESTIMATE = Gauge("tm_agg_estimate", "Latest regional estimate", ["region", "algorithm"])
WINDOW_SENSORS = Gauge("tm_agg_window_sensors", "Sensors in the last closed window", ["region"])
MQTT_CONNECTED = Gauge("tm_agg_mqtt_connected", "1 when connected to the broker")
DB_CONNECTED = Gauge("tm_agg_db_connected", "1 when connected to the database")
DB_BUFFER = Gauge("tm_agg_db_buffer_rows", "Rows waiting to be written", ["kind"])
DB_DROPPED = Counter("tm_agg_db_dropped_total", "Reading rows dropped because the buffer overflowed")
INGEST_DELAY = Histogram("tm_agg_ingest_delay_seconds", "Sensor timestamp to arrival delay",
                         buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))
WINDOW_LATENCY = Histogram("tm_agg_window_latency_seconds", "Window end to estimate stored",
                           buckets=(0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 20))
DB_WRITE = Histogram("tm_agg_db_write_seconds", "Duration of one batch write", ["kind"],
                     buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))

INSERT_READING = """
INSERT INTO readings (sensor_id, seq, region, ts, value, received_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (sensor_id, seq) DO NOTHING
"""
UPSERT_ESTIMATE = """
INSERT INTO estimates (region, window_start, window_ms, algorithm, estimate,
                       n_readings, n_sensors, replica_id, epoch)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (region, window_start, algorithm) DO UPDATE SET
  estimate = EXCLUDED.estimate, n_readings = EXCLUDED.n_readings, n_sensors = EXCLUDED.n_sensors,
  replica_id = EXCLUDED.replica_id, epoch = EXCLUDED.epoch, computed_at = now()
WHERE estimates.epoch <= EXCLUDED.epoch
"""  # the epoch condition is the fencing hook used in Stage 3

READING_CHUNK = 2000
MAX_BUFFERED_READINGS = 100_000
SEEN_PER_SENSOR = 128
FUTURE_TOLERANCE_MS = 60_000


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class Aggregator:
    def __init__(self, cfg: Config, log):
        self.cfg = cfg
        self.log = log
        self.window_ms = cfg.window_ms
        self.regions = cfg.region_filter
        self.epoch = 0
        # (region, window_start_ms) -> sensor_id -> values seen in that window
        self.windows: dict[tuple[str, int], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        self.window_counts: dict[tuple[str, int], int] = defaultdict(int)
        # The window in progress at startup is incomplete, so start with the next one.
        start = now_ms()
        self.accept_from = start - start % self.window_ms + self.window_ms
        self.closed_up_to = self.accept_from
        self.seen: dict[str, tuple[deque, set]] = {}
        self.reading_rows: list[tuple] = []
        self.estimate_rows: list[tuple[tuple, int]] = []
        self.invalid_logged = 0
        self.flush_now = asyncio.Event()  # set when estimates are ready, so they are written at once

    # ---------- ingestion ----------
    def _is_duplicate(self, sensor_id: str, seq: int) -> bool:
        entry = self.seen.get(sensor_id)
        if entry is None:
            entry = (deque(maxlen=SEEN_PER_SENSOR), set())
            self.seen[sensor_id] = entry
        recent, lookup = entry
        if seq in lookup:
            return True
        if len(recent) == recent.maxlen:
            lookup.discard(recent[0])
        recent.append(seq)
        lookup.add(seq)
        return False

    def _reject(self, reason: str) -> str:
        INVALID.inc()
        self.invalid_logged += 1
        if self.invalid_logged <= 20 or self.invalid_logged % 100 == 0:
            self.log.warning("invalid reading #%d sent to dead-letter: %s", self.invalid_logged, reason)
        return "invalid"

    def handle(self, payload, recv_ms: int) -> str:
        try:
            r = parse_reading(payload)
        except ValueError as exc:
            return self._reject(str(exc))
        if r.ts > recv_ms + FUTURE_TOLERANCE_MS:
            return self._reject(f"timestamp {r.ts} is in the future")
        if self.regions is not None and r.region not in self.regions:
            return "ignored"
        if self._is_duplicate(r.sensor_id, r.seq):
            DUPLICATES.inc()
            return "duplicate"

        READINGS.labels(r.region).inc()
        INGEST_DELAY.observe(max(0.0, (recv_ms - r.ts) / 1000))
        self.reading_rows.append((r.sensor_id, r.seq, r.region, to_dt(r.ts), r.value, to_dt(recv_ms)))
        if len(self.reading_rows) > MAX_BUFFERED_READINGS:
            overflow = len(self.reading_rows) - MAX_BUFFERED_READINGS
            del self.reading_rows[:overflow]
            DB_DROPPED.inc(overflow)

        window_start = r.ts - r.ts % self.window_ms
        if window_start < self.closed_up_to:
            if window_start >= self.accept_from:
                LATE.inc()
            return "late"
        key = (r.region, window_start)
        self.windows[key][r.sensor_id].append(r.value)
        self.window_counts[key] += 1
        return "ok"

    async def consume(self) -> None:
        topics = ([f"readings/{r}/#" for r in sorted(self.regions)] if self.regions else ["readings/#"])
        backoff = 1.0
        while True:
            try:
                async with aiomqtt.Client(self.cfg.mqtt_host, self.cfg.mqtt_port,
                                          identifier=f"agg-{self.cfg.instance_id}",
                                          clean_session=False,
                                          logger=mqtt_logger()) as client:
                    for topic in topics:
                        await client.subscribe(topic, qos=self.cfg.mqtt_qos)
                    MQTT_CONNECTED.set(1)
                    backoff = 1.0
                    self.log.info("subscribed to %s", topics)
                    async for message in client.messages:
                        if self.handle(message.payload, now_ms()) == "invalid":
                            await client.publish(DEADLETTER_TOPIC, message.payload, qos=0)
            except aiomqtt.MqttError as exc:
                MQTT_CONNECTED.set(0)
                self.log.warning("broker connection lost (%s); reconnecting in %.0fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    # ---------- windowing ----------
    def close_due_windows(self) -> None:
        now = now_ms()
        horizon = now - self.cfg.grace_ms
        self.closed_up_to = max(self.closed_up_to, horizon - horizon % self.window_ms)
        due = sorted((k for k in self.windows if k[1] < self.closed_up_to), key=lambda k: k[1])
        for key in due:
            per_sensor = self.windows.pop(key)
            n_readings = self.window_counts.pop(key, 0)
            region, window_start = key
            # Average each sensor first so every sensor counts once, however often it reported.
            values = np.fromiter((sum(v) / len(v) for v in per_sensor.values()),
                                 dtype=float, count=len(per_sensor))
            window_end = window_start + self.window_ms
            for name, fn in ALGORITHMS.items():
                estimate = fn(values)
                row = (region, to_dt(window_start), self.window_ms, name, estimate,
                       n_readings, len(values), self.cfg.instance_id, self.epoch)
                self.estimate_rows.append((row, window_end))
                ESTIMATE.labels(region, name).set(estimate)
            WINDOW_SENSORS.labels(region).set(len(values))
            WINDOWS_CLOSED.labels(region).inc()
        if due:
            self.flush_now.set()

    async def close_loop(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            try:
                self.close_due_windows()
            except Exception:
                self.log.exception("window close failed; continuing")

    # ---------- storage ----------
    async def write_loop(self) -> None:
        conn: psycopg.AsyncConnection | None = None
        backoff = 1.0
        while True:
            # Wake up on the flush timer, or immediately when a window has closed.
            try:
                await asyncio.wait_for(self.flush_now.wait(), timeout=self.cfg.db_flush_interval_s)
            except TimeoutError:
                pass
            self.flush_now.clear()
            DB_BUFFER.labels("readings").set(len(self.reading_rows))
            DB_BUFFER.labels("estimates").set(len(self.estimate_rows))
            if not self.reading_rows and not self.estimate_rows:
                continue
            try:
                if conn is None or conn.closed:
                    conn = await psycopg.AsyncConnection.connect(self.cfg.pg_dsn, autocommit=True)
                    DB_CONNECTED.set(1)
                    self.log.info("connected to database")
                    backoff = 1.0
                await self._flush_estimates(conn)
                await self._flush_readings(conn)
            except psycopg.Error as exc:
                DB_CONNECTED.set(0)
                self.log.warning("database write failed (%s); retrying in %.0fs", exc, backoff)
                if conn is not None:
                    await conn.close()
                conn = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _flush_estimates(self, conn: psycopg.AsyncConnection) -> None:
        if not self.estimate_rows:
            return
        batch, self.estimate_rows = self.estimate_rows, []
        started = time.perf_counter()
        try:
            async with conn.cursor() as cur:
                await cur.executemany(UPSERT_ESTIMATE, [row for row, _ in batch])
        except BaseException:
            self.estimate_rows = batch + self.estimate_rows  # keep them for the retry
            raise
        DB_WRITE.labels("estimates").observe(time.perf_counter() - started)
        stored = now_ms()
        for _, window_end in batch[::len(ALGORITHMS)]:
            WINDOW_LATENCY.observe(max(0.0, (stored - window_end) / 1000))

    async def _flush_readings(self, conn: psycopg.AsyncConnection) -> None:
        while self.reading_rows:
            chunk = self.reading_rows[:READING_CHUNK]
            started = time.perf_counter()
            async with conn.cursor() as cur:
                await cur.executemany(INSERT_READING, chunk)  # idempotent: safe to retry
            del self.reading_rows[:len(chunk)]
            DB_WRITE.labels("readings").observe(time.perf_counter() - started)


async def main() -> None:
    cfg = load_config("aggregator")
    log = setup_logging(cfg)
    start_http_server(cfg.metrics_port)
    agg = Aggregator(cfg, log)
    log.info("aggregator %s starting: regions=%s window=%dms grace=%dms algorithms=%s",
             cfg.instance_id, cfg.regions, cfg.window_ms, cfg.grace_ms, list(ALGORITHMS))
    async with asyncio.TaskGroup() as tg:
        tg.create_task(agg.consume())
        tg.create_task(agg.close_loop())
        tg.create_task(agg.write_loop())


if __name__ == "__main__":
    asyncio.run(main())
