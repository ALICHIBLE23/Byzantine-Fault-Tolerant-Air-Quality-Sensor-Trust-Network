"""Regional aggregator replica (Stage 4: replicated, leader-based, with border exchange).

Each replica serves a partition of regions (REGIONS env, e.g. "r-0-0,r-0-1") and competes for one
leader lease per region in Redis (services/aggregator/leases.py).

  leader  of a region: computes the region's windows AND writes estimates, sensor trust, trust
                       history and raw readings, tagged with its lease epoch (fencing token);
                       saves the trust state to Redis after every window.
  standby of a region: computes exactly the same windows from the same MQTT stream (hot standby),
                       but writes nothing; it keeps the last few windows in a backlog.

On promotion a standby adopts the previous leader's trust state from Redis and writes its backlog,
so no window is lost during failover. Every database write runs in a transaction that first reads
the region's newest published epoch (row-locked); a write from an older epoch is rejected.

Border exchange: a replica also receives readings of sensors in neighbouring regions within the trust
radius of its own regions (subscribing only to the border cells, readings/<region>/<cell>/#). They act
as spatial neighbours, so sensors at an edge are judged against their full neighbourhood. A border
sensor is evaluated by every region it borders, and its reputation is one shared record in Redis that
all evaluating leaders update (services/aggregator/reputation.py, concurrency strategy CONCURRENCY).

Concurrent loops, each with its own error handling and reconnection:
  leases   - acquire / renew / lose leader leases
  consume  - MQTT -> validate -> deduplicate -> place into time windows
  close    - when a window's grace period has passed, run the trust algorithm and baselines
  write    - publish new epochs, then fenced batch writes to PostgreSQL
  state    - save trust state of led regions to Redis
"""
from __future__ import annotations

import asyncio
import signal
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import aiomqtt
import numpy as np
import psycopg
import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from redis.exceptions import RedisError

from common.aggregation import ALGORITHMS
from common.config import Config, load_config
from common.geo import all_regions, border_cells, distance_to_region, regions_within
from common.logclient import mqtt_logger, setup_logging
from common.schemas import DEADLETTER_TOPIC, parse_reading
from common.trust import TrustEngine, TrustParams
from services.aggregator.leases import LeaseManager, RedisLeaseBackend
from services.aggregator.reputation import SharedReputation
from services.aggregator.trust_store import RedisTrustStore

# ---- pipeline metrics
READINGS = Counter("tm_agg_readings_total", "Valid, unique readings received", ["region"])
DUPLICATES = Counter("tm_agg_duplicates_total", "Duplicate readings dropped")
LATE = Counter("tm_agg_late_total", "Readings that arrived after their window closed")
INVALID = Counter("tm_agg_invalid_total", "Malformed readings sent to the dead-letter topic")
WINDOWS_CLOSED = Counter("tm_agg_windows_closed_total", "Windows aggregated", ["region"])
ESTIMATE = Gauge("tm_agg_estimate", "Latest regional estimate (leader only)", ["region", "algorithm"])
WINDOW_SENSORS = Gauge("tm_agg_window_sensors", "Sensors in the last closed window (leader only)", ["region"])
MQTT_CONNECTED = Gauge("tm_agg_mqtt_connected", "1 when connected to the broker")
DB_CONNECTED = Gauge("tm_agg_db_connected", "1 when connected to the database")
DB_BUFFER = Gauge("tm_agg_db_buffer_rows", "Rows waiting to be written", ["kind"])
DB_DROPPED = Counter("tm_agg_db_dropped_total", "Rows dropped because a buffer overflowed")
INGEST_DELAY = Histogram("tm_agg_ingest_delay_seconds", "Sensor timestamp to arrival delay",
                         buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))
WINDOW_LATENCY = Histogram("tm_agg_window_latency_seconds", "Window end to estimate stored",
                           buckets=(0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 20))
DB_WRITE = Histogram("tm_agg_db_write_seconds", "Duration of one batch write", ["kind"],
                     buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))
# ---- trust metrics
FLAGGED = Gauge("tm_trust_flagged", "Sensors currently flagged as untrustworthy (leader only)", ["region"])
CONFIDENCE = Gauge("tm_trust_confidence", "Share of trusted reputation behind the estimate", ["region"])
FLAG_EVENTS = Counter("tm_trust_flag_events_total", "Sensors newly flagged", ["region", "reason"])
UNFLAG_EVENTS = Counter("tm_trust_unflag_events_total", "Sensors that regained trust", ["region"])
TRUST_SECONDS = Histogram("tm_trust_compute_seconds", "Time to run the trust algorithm for one window",
                          buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25))
# ---- distribution metrics
IS_LEADER = Gauge("tm_agg_is_leader", "1 if this replica holds the region's leader lease", ["region"])
EPOCH = Gauge("tm_agg_epoch", "Fencing epoch of the lease this replica holds (0 = standby)", ["region"])
LEADER_CHANGES = Counter("tm_agg_leader_changes_total", "Times this replica became leader", ["region"])
FENCED = Counter("tm_agg_fenced_writes_total", "Rows rejected because a newer leader epoch exists", ["kind"])
NOT_LEADER_DROPS = Counter("tm_agg_not_leader_drops_total", "Buffered rows discarded after losing leadership")
BACKLOG_HANDOVER = Counter("tm_agg_backlog_windows_total", "Standby windows written after promotion", ["region"])
BORDER_READINGS = Counter("tm_agg_border_readings_total", "Readings from neighbouring regions' border sensors",
                          ["region"])
BORDER_SENSORS = Gauge("tm_agg_border_sensors", "Foreign border sensors used as context in the last window",
                       ["region"])
SHARED_ERRORS = Counter("tm_agg_shared_reputation_errors_total", "Shared reputation reads/updates that failed",
                        ["op"])
STATE_SAVES = Counter("tm_agg_trust_state_saves_total", "Trust state snapshots saved to Redis", ["result"])

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
"""
UPSERT_TRUST = """
INSERT INTO sensor_trust (sensor_id, region, reputation, spatial, temporal, value, reference,
                          flagged, reason, window_start, replica_id, epoch)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (sensor_id) DO UPDATE SET
  region = EXCLUDED.region, reputation = EXCLUDED.reputation, spatial = EXCLUDED.spatial,
  temporal = EXCLUDED.temporal, value = EXCLUDED.value, reference = EXCLUDED.reference,
  flagged = EXCLUDED.flagged, reason = EXCLUDED.reason, window_start = EXCLUDED.window_start,
  replica_id = EXCLUDED.replica_id, epoch = EXCLUDED.epoch, updated_at = now()
WHERE sensor_trust.window_start <= EXCLUDED.window_start
"""
INSERT_HISTORY = """
INSERT INTO trust_history (window_start, sensor_id, region, reputation, flagged)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (sensor_id, window_start) DO NOTHING
"""
PUBLISH_EPOCH = """
INSERT INTO region_epochs (region, epoch, leader) VALUES (%s, %s, %s)
ON CONFLICT (region) DO UPDATE SET
  leader = CASE WHEN EXCLUDED.epoch >= region_epochs.epoch THEN EXCLUDED.leader ELSE region_epochs.leader END,
  epoch = GREATEST(region_epochs.epoch, EXCLUDED.epoch),
  updated_at = now()
RETURNING epoch
"""
CURRENT_EPOCH_LOCKED = "SELECT epoch FROM region_epochs WHERE region = %s FOR SHARE"

READING_CHUNK = 2000
MAX_BUFFERED_READINGS = 100_000
MAX_BUFFERED_TRUST = 50_000
SEEN_PER_SENSOR = 128
FUTURE_TOLERANCE_MS = 60_000
BACKLOG_WINDOWS = 3
READING_BACKLOG = 1500
ALL_ALGORITHMS = [*ALGORITHMS, "trustmesh"]
TRUST_RADIUS_KM = TrustParams().radius_km
MAX_PARALLEL_UPDATES = 32


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def with_epoch(row: tuple, epoch: int) -> tuple:
    """Rows keep their epoch as the last field."""
    return row[:-1] + (epoch,)


class Aggregator:
    def __init__(self, cfg: Config, log, leases, store: RedisTrustStore | None = None,
                 reputations: SharedReputation | None = None):
        self.cfg = cfg
        self.log = log
        self.leases = leases
        self.store = store
        self.reputations = reputations if cfg.border_exchange else None
        self.window_ms = cfg.window_ms
        self.regions = sorted(cfg.region_filter or all_regions(cfg))
        self.region_set = set(self.regions)
        # (region, window_start_ms) -> sensor_id -> values seen in that window
        self.windows: dict[tuple[str, int], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        self.window_counts: dict[tuple[str, int], int] = defaultdict(int)
        start = now_ms()
        self.accept_from = start - start % self.window_ms + self.window_ms  # skip the partial first window
        self.closed_up_to = self.accept_from
        self.seen: dict[str, tuple[deque, set]] = {}
        self.positions: dict[str, tuple[float, float]] = {}
        self.sensor_region: dict[str, str] = {}
        self.targets: dict[str, list[str]] = {}     # sensor -> my regions whose windows it belongs to
        self.is_border: dict[str, bool] = {}        # sensor -> evaluated by more than one region?
        self.evaluators: dict[str, int] = {}        # sensor -> number of regions that evaluate it
        self.engines: dict[str, TrustEngine] = defaultdict(TrustEngine)  # one per region
        # Write buffers (leader). Every estimate / trust / history row ends with its epoch.
        self.reading_rows: list[tuple] = []
        self.estimate_rows: list[tuple[tuple, int | None]] = []
        self.trust_rows: list[tuple] = []
        self.history_rows: list[tuple] = []
        # Standby backlog: what we computed but did not write, handed over on promotion.
        self.backlog: dict[str, deque] = defaultdict(lambda: deque(maxlen=BACKLOG_WINDOWS))
        self.reading_backlog: dict[str, deque] = defaultdict(lambda: deque(maxlen=READING_BACKLOG))
        self.epochs_to_publish: dict[str, int] = {}
        self.dirty_state: set[str] = set()
        self.invalid_logged = 0
        self.flush_now = asyncio.Event()

    # ---------------- leadership ----------------
    async def on_promote(self, region: str, epoch: int) -> None:
        LEADER_CHANGES.labels(region).inc()
        IS_LEADER.labels(region).set(1)
        EPOCH.labels(region).set(epoch)
        self.epochs_to_publish[region] = epoch
        adopted = 0
        if self.store is not None:
            try:
                state = await self.store.load(region)
                if state:
                    self.engines[region].import_state(state)
                    adopted = len(state)
            except RedisError as exc:
                self.log.warning("could not load trust state for %s (%s); keeping own state", region, exc)
        handed = 0
        for item in self.backlog.pop(region, deque()):
            # No latency sample for these: they are re-writes of recent windows (most were already stored
            # by the old leader), so their "age" would inflate the window-latency metric.
            self.estimate_rows.extend((with_epoch(row, epoch), None) for row, _ in item["estimates"])
            self.trust_rows.extend(with_epoch(row, epoch) for row in item["trust"])
            self.history_rows.extend(with_epoch(row, epoch) for row in item["history"])
            handed += 1
        self.reading_rows.extend(self.reading_backlog.pop(region, deque()))
        BACKLOG_HANDOVER.labels(region).inc(handed)
        self.log.warning("now leader of %s (epoch %d): adopted trust state of %d sensors, handed over %d "
                         "standby windows", region, epoch, adopted, handed)
        self.flush_now.set()

    async def on_demote(self, region: str, reason: str) -> None:
        IS_LEADER.labels(region).set(0)
        EPOCH.labels(region).set(0)
        for gauge, labels in [(FLAGGED, (region,)), (CONFIDENCE, (region,)), (WINDOW_SENSORS, (region,))] + \
                             [(ESTIMATE, (region, a)) for a in ALL_ALGORITHMS]:
            try:
                gauge.remove(*labels)
            except KeyError:
                pass
        self.dirty_state.discard(region)
        if self.leases.self_check:
            self._drop_region_rows(region)
        # With FENCING_SELF_CHECK=false the stale rows are kept on purpose, to show that the
        # database rejects them (fencing experiment).

    def _drop_region_rows(self, region: str) -> None:
        before = len(self.estimate_rows) + len(self.trust_rows) + len(self.history_rows) + len(self.reading_rows)
        self.estimate_rows = [(r, e) for r, e in self.estimate_rows if r[0] != region]
        self.trust_rows = [r for r in self.trust_rows if r[1] != region]
        self.history_rows = [r for r in self.history_rows if r[2] != region]
        self.reading_rows = [r for r in self.reading_rows if r[2] != region]
        after = len(self.estimate_rows) + len(self.trust_rows) + len(self.history_rows) + len(self.reading_rows)
        NOT_LEADER_DROPS.inc(before - after)

    async def restore_state(self) -> None:
        """At startup, load every region's trust state so a restart does not reset reputations."""
        if self.store is None:
            return
        for attempt in range(10):
            try:
                for region in self.regions:
                    state = await self.store.load(region)
                    if state:
                        self.engines[region].import_state(state)
                        self.log.info("restored trust state of %d sensors for %s", len(state), region)
                return
            except RedisError as exc:
                self.log.warning("cannot load trust state yet (%s); retry %d/10", exc, attempt + 1)
                await asyncio.sleep(min(2 ** attempt, 10))
        self.log.error("starting without saved trust state: Redis unreachable")

    # ---------------- ingestion ----------------
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
        targets = self._targets_for(r.sensor_id, r.region, r.x, r.y)
        if not targets:
            return "ignored"
        if self._is_duplicate(r.sensor_id, r.seq):
            DUPLICATES.inc()
            return "duplicate"

        own = r.region in self.region_set
        if not own:
            BORDER_READINGS.labels(r.region).inc()
        else:
            READINGS.labels(r.region).inc()
            INGEST_DELAY.observe(max(0.0, (recv_ms - r.ts) / 1000))
        row = (r.sensor_id, r.seq, r.region, to_dt(r.ts), r.value, to_dt(recv_ms))
        if not own:
            pass  # the owner's leader stores foreign readings
        elif self.leases.is_leader(r.region):
            self.reading_rows.append(row)
            if len(self.reading_rows) > MAX_BUFFERED_READINGS:
                overflow = len(self.reading_rows) - MAX_BUFFERED_READINGS
                del self.reading_rows[:overflow]
                DB_DROPPED.inc(overflow)
        else:
            self.reading_backlog[r.region].append(row)

        window_start = r.ts - r.ts % self.window_ms
        if window_start < self.closed_up_to:
            if window_start >= self.accept_from:
                LATE.inc()
            return "late"
        self.positions[r.sensor_id] = (r.x, r.y)
        for region in targets:
            key = (region, window_start)
            self.windows[key][r.sensor_id].append(r.value)
            if region == r.region:
                self.window_counts[key] += 1
        return "ok" if own else "border"

    def topics(self) -> list[str]:
        topics = [f"readings/{r}/#" for r in self.regions]
        if self.cfg.border_exchange:
            foreign = {(fr, cell) for r in self.regions for fr, cell in border_cells(r, self.cfg, TRUST_RADIUS_KM)
                       if fr not in self.region_set}
            topics += [f"readings/{fr}/{cell}/#" for fr, cell in sorted(foreign)]
        return topics

    def _targets_for(self, sensor_id: str, region: str, x: float, y: float) -> list[str]:
        """My regions whose windows this sensor belongs to (its own region if mine, plus my regions
        within the trust radius when border exchange is on). Cached: sensors do not move."""
        targets = self.targets.get(sensor_id)
        if targets is None:
            targets = [region] if region in self.region_set else []
            if self.cfg.border_exchange:
                targets += [r for r in self.regions
                            if r != region and distance_to_region(x, y, r, self.cfg) <= TRUST_RADIUS_KM]
                neighbours = regions_within(x, y, self.cfg, TRUST_RADIUS_KM)
                self.is_border[sensor_id] = bool(neighbours)
                self.evaluators[sensor_id] = 1 + len(neighbours)
            else:
                self.is_border[sensor_id] = False
                self.evaluators[sensor_id] = 1
            self.targets[sensor_id] = targets
            self.sensor_region[sensor_id] = region
        return targets

    async def consume(self) -> None:
        topics = self.topics()
        backoff = 1.0
        while True:
            try:
                async with aiomqtt.Client(self.cfg.mqtt_host, self.cfg.mqtt_port,
                                          identifier=f"agg-{self.cfg.instance_id}",
                                          clean_session=False, logger=mqtt_logger()) as client:
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

    # ---------------- windowing ----------------
    async def close_due_windows(self) -> None:
        now = now_ms()
        horizon = now - self.cfg.grace_ms
        self.closed_up_to = max(self.closed_up_to, horizon - horizon % self.window_ms)
        due = sorted((k for k in self.windows if k[1] < self.closed_up_to), key=lambda k: k[1])
        for key in due:
            await self._close_window(key)
        if due:
            self.flush_now.set()

    async def _close_window(self, key: tuple[str, int]) -> None:
        per_sensor = self.windows.pop(key)   # popped first: readings arriving during the awaits go elsewhere
        n_readings = self.window_counts.pop(key, 0)
        region, window_start = key
        sensor_ids = list(per_sensor)
        owned = np.array([self.sensor_region.get(s) == region for s in sensor_ids])
        if not owned.any():
            return  # only neighbours' border sensors reported in this window
        # Average each sensor first so every sensor counts once, however often it reported.
        values = np.fromiter((sum(v) / len(v) for v in per_sensor.values()), dtype=float, count=len(per_sensor))
        xs = np.array([self.positions[s][0] for s in sensor_ids])
        ys = np.array([self.positions[s][1] for s in sensor_ids])
        ws_dt = to_dt(window_start)
        BORDER_SENSORS.labels(region).set(int((~owned).sum()))

        border_ids = [s for s in sensor_ids if self.is_border.get(s)]
        shared: dict[str, float | None] = {}
        if self.reputations is not None and border_ids:
            try:
                shared = await self.reputations.read_many(border_ids)
            except RedisError as exc:
                SHARED_ERRORS.labels("read").inc()
                self.log.warning("shared reputations unavailable for %s (%s); using local state", region, exc)

        engine = self.engines[region]
        was_flagged = {s for s in sensor_ids if s in engine.states and engine.states[s].flagged}
        started = time.perf_counter()
        result = engine.process_window(sensor_ids, xs, ys, values, owned=owned, shared=shared)
        TRUST_SECONDS.observe(time.perf_counter() - started)
        WINDOWS_CLOSED.labels(region).inc()

        leader = self.leases.is_leader(region)
        epoch = self.leases.epoch(region) if leader else 0
        if leader and self.reputations is not None and border_ids:
            await self._submit_evidence(region, window_start, engine, result)
        own_values = values[owned]
        estimates = {name: fn(own_values) for name, fn in ALGORITHMS.items()}
        estimates["trustmesh"] = result.estimate
        window_end = window_start + self.window_ms
        est_rows = [((region, ws_dt, self.window_ms, name, value, n_readings, len(own_values),
                      self.cfg.instance_id, epoch), window_end if i == 0 else None)
                    for i, (name, value) in enumerate(estimates.items())]
        trust_rows, history_rows = [], []
        own_verdicts = [v for v in result.verdicts if v.owned]
        for v in own_verdicts:
            trust_rows.append((v.sensor_id, region, v.reputation, v.spatial, v.temporal, v.value, v.reference,
                               v.flagged, v.reason, ws_dt, self.cfg.instance_id, epoch))
            history_rows.append((ws_dt, v.sensor_id, region, v.reputation, v.flagged, epoch))

        if not leader:
            self.backlog[region].append({"estimates": est_rows, "trust": trust_rows, "history": history_rows})
            return

        self._log_flag_changes(region, result, was_flagged)
        self.estimate_rows.extend(est_rows)
        self.trust_rows.extend(trust_rows)
        self.history_rows.extend(history_rows)
        for rows in (self.trust_rows, self.history_rows):
            if len(rows) > MAX_BUFFERED_TRUST:
                overflow = len(rows) - MAX_BUFFERED_TRUST
                del rows[:overflow]
                DB_DROPPED.inc(overflow)
        for name, value in estimates.items():
            ESTIMATE.labels(region, name).set(value)
        WINDOW_SENSORS.labels(region).set(len(own_values))
        FLAGGED.labels(region).set(result.n_flagged)
        CONFIDENCE.labels(region).set(result.confidence)
        self.dirty_state.add(region)

    async def _submit_evidence(self, region: str, window_start: int, engine: TrustEngine, result) -> None:
        """Apply this window's evidence for every border sensor to its shared reputation, then adopt the
        merged value (which also contains the other regions' evidence)."""
        limit = asyncio.Semaphore(MAX_PARALLEL_UPDATES)

        async def one(v) -> None:
            async with limit:
                try:
                    merged, _, _, _ = await self.reputations.apply(
                        v.sensor_id, v.score, region, window_start, prior=v.prior_reputation,
                        weight=1.0 / self.evaluators.get(v.sensor_id, 1))
                    engine.state(v.sensor_id).reputation = merged
                except Exception as exc:
                    SHARED_ERRORS.labels("update").inc()
                    self.log.warning("shared reputation update for %s failed (%s)", v.sensor_id, exc)

        await asyncio.gather(*(one(v) for v in result.verdicts if self.is_border.get(v.sensor_id)))

    def _log_flag_changes(self, region: str, result, was_flagged: set[str]) -> None:
        for v in result.verdicts:
            if not v.owned:
                continue
            if v.flagged and v.sensor_id not in was_flagged:
                FLAG_EVENTS.labels(region, v.reason or "low reputation").inc()
                ref = f"{v.reference:.1f}" if v.reference is not None else "n/a"
                self.log.warning("sensor %s flagged in %s: %s (reputation %.2f, reported %.1f, neighbours %s)",
                                 v.sensor_id, region, v.reason or "low reputation", v.reputation, v.value, ref)
            elif not v.flagged and v.sensor_id in was_flagged:
                UNFLAG_EVENTS.labels(region).inc()
                self.log.info("sensor %s in %s regained trust (reputation %.2f)", v.sensor_id, region, v.reputation)

    async def close_loop(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            try:
                await self.close_due_windows()
            except Exception:
                self.log.exception("window close failed; continuing")

    # ---------------- trust state in Redis ----------------
    async def state_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if self.store is None:
                continue
            for region in list(self.dirty_state):
                if not self.leases.is_leader(region):
                    self.dirty_state.discard(region)
                    continue
                try:
                    saved = await self.store.save(region, self.cfg.instance_id, self.engines[region].export_state())
                    self.dirty_state.discard(region)
                    STATE_SAVES.labels("ok" if saved else "fenced").inc()
                    if not saved:
                        self.log.warning("trust state save for %s rejected: no longer the lease holder", region)
                except RedisError as exc:
                    STATE_SAVES.labels("error").inc()
                    self.log.warning("trust state save for %s failed (%s)", region, exc)

    # ---------------- storage ----------------
    async def write_loop(self) -> None:
        conn: psycopg.AsyncConnection | None = None
        backoff = 1.0
        while True:
            try:
                await asyncio.wait_for(self.flush_now.wait(), timeout=self.cfg.db_flush_interval_s)
            except TimeoutError:
                pass
            self.flush_now.clear()
            if self.leases.self_check:
                for region in self.regions:
                    if not self.leases.is_leader(region):
                        self._drop_region_rows(region)
            DB_BUFFER.labels("readings").set(len(self.reading_rows))
            DB_BUFFER.labels("estimates").set(len(self.estimate_rows))
            DB_BUFFER.labels("trust").set(len(self.trust_rows))
            if not (self.reading_rows or self.estimate_rows or self.trust_rows or self.history_rows
                    or self.epochs_to_publish):
                continue
            try:
                if conn is None or conn.closed:
                    conn = await psycopg.AsyncConnection.connect(self.cfg.pg_dsn, autocommit=True)
                    DB_CONNECTED.set(1)
                    self.log.info("connected to database")
                    backoff = 1.0
                await self._publish_epochs(conn)
                await self._flush_estimates(conn)
                await self._flush_trust(conn)
                await self._flush_readings(conn)
            except psycopg.Error as exc:
                DB_CONNECTED.set(0)
                self.log.warning("database write failed (%s); retrying in %.0fs", exc, backoff)
                if conn is not None:
                    await conn.close()
                conn = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _publish_epochs(self, conn: psycopg.AsyncConnection) -> None:
        """A new leader publishes its epoch BEFORE its first write; from then on older epochs are fenced."""
        for region, epoch in list(self.epochs_to_publish.items()):
            cur = await conn.execute(PUBLISH_EPOCH, (region, epoch, self.cfg.instance_id))
            current = (await cur.fetchone())[0]
            if current > epoch:
                self.log.error("epoch for %s in the database (%d) is newer than ours (%d): Redis and PostgreSQL "
                               "are out of sync (wipe both together with `docker compose down -v`)",
                               region, current, epoch)
            del self.epochs_to_publish[region]

    async def _fenced_executemany(self, conn, kind: str, sql: str, rows: list[tuple], region_idx: int,
                                  strip_epoch: bool = False) -> None:
        """Write rows grouped by (region, epoch). Each group runs in a transaction that row-locks the region's
        published epoch: if a newer leader exists the group is rejected. The lock also makes a new leader's
        epoch publication wait until this write commits, so the two can never interleave."""
        groups: dict[tuple[str, int], list[tuple]] = defaultdict(list)
        for row in rows:
            groups[(row[region_idx], row[-1])].append(row)
        for (region, epoch), group in groups.items():
            async with conn.transaction():
                cur = await conn.execute(CURRENT_EPOCH_LOCKED, (region,))
                found = await cur.fetchone()
                current = found[0] if found else 0
                if current > epoch:
                    FENCED.labels(kind).inc(len(group))
                    self.log.warning("fenced: rejected %d %s rows for %s from stale epoch %d (current %d)",
                                     len(group), kind, region, epoch, current)
                    continue
                async with conn.cursor() as c:
                    await c.executemany(sql, [r[:-1] for r in group] if strip_epoch else group)

    async def _flush_estimates(self, conn: psycopg.AsyncConnection) -> None:
        if not self.estimate_rows:
            return
        batch, self.estimate_rows = self.estimate_rows, []
        started = time.perf_counter()
        try:
            await self._fenced_executemany(conn, "estimates", UPSERT_ESTIMATE, [r for r, _ in batch], region_idx=0)
        except BaseException:
            self.estimate_rows = batch + self.estimate_rows  # keep them for the retry
            raise
        DB_WRITE.labels("estimates").observe(time.perf_counter() - started)
        stored = now_ms()
        for _, window_end in batch:
            if window_end is not None:
                WINDOW_LATENCY.observe(max(0.0, (stored - window_end) / 1000))

    async def _flush_trust(self, conn: psycopg.AsyncConnection) -> None:
        if not self.trust_rows and not self.history_rows:
            return
        # Swap the buffers out BEFORE awaiting: the close loop may append new rows meanwhile.
        trust, self.trust_rows = self.trust_rows, []
        history, self.history_rows = self.history_rows, []
        latest = {row[0]: row for row in trust}  # only the newest row per sensor matters for current state
        started = time.perf_counter()
        try:
            await self._fenced_executemany(conn, "trust", UPSERT_TRUST, list(latest.values()), region_idx=1)
            await self._fenced_executemany(conn, "history", INSERT_HISTORY, history, region_idx=2, strip_epoch=True)
        except BaseException:
            self.trust_rows = trust + self.trust_rows
            self.history_rows = history + self.history_rows
            raise
        DB_WRITE.labels("trust").observe(time.perf_counter() - started)

    async def _flush_readings(self, conn: psycopg.AsyncConnection) -> None:
        while self.reading_rows:
            chunk = self.reading_rows[:READING_CHUNK]
            started = time.perf_counter()
            async with conn.cursor() as cur:
                await cur.executemany(INSERT_READING, chunk)  # idempotent: overlap after failover is harmless
            del self.reading_rows[:len(chunk)]
            DB_WRITE.labels("readings").observe(time.perf_counter() - started)


async def main() -> None:
    cfg = load_config("aggregator")
    log = setup_logging(cfg)
    start_http_server(cfg.metrics_port)
    redis = aioredis.Redis(host=cfg.redis_host, port=cfg.redis_port, socket_timeout=2, socket_connect_timeout=2)
    regions = sorted(cfg.region_filter or all_regions(cfg))
    leases = LeaseManager(RedisLeaseBackend(redis), regions, cfg.instance_id, ttl_ms=cfg.lease_ttl_ms,
                          renew_interval_s=cfg.lease_renew_ms / 1000, self_check=cfg.fencing_self_check, log=log)
    reputations = SharedReputation(redis, cfg.concurrency, think_ms=cfg.concurrency_think_ms)
    agg = Aggregator(cfg, log, leases, RedisTrustStore(redis), reputations)
    leases.on_promote = agg.on_promote
    leases.on_demote = agg.on_demote
    for region in regions:
        IS_LEADER.labels(region).set(0)
        EPOCH.labels(region).set(0)
    log.info("aggregator %s starting: regions=%s lease_ttl=%dms self_check=%s border_exchange=%s "
             "concurrency=%s algorithms=%s", cfg.instance_id, regions, cfg.lease_ttl_ms, cfg.fencing_self_check,
             cfg.border_exchange, cfg.concurrency, ALL_ALGORITHMS)

    # docker stop sends SIGTERM: release leases so a standby takes over at once (no TTL wait).
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, main_task.cancel)

    await agg.restore_state()
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(leases.run())
            tg.create_task(agg.consume())
            tg.create_task(agg.close_loop())
            tg.create_task(agg.write_loop())
            tg.create_task(agg.state_loop())
    except asyncio.CancelledError:
        log.warning("shutting down: releasing leases %s for immediate failover", sorted(leases.held))
        await leases.release_all()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
