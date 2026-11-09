"""Shared reputation of border sensors, with four concurrency-control strategies.

A sensor near a region boundary is evaluated by two (or more) regions, possibly led by different
aggregator processes. After each window, every evaluating leader applies its evidence (score q) to
ONE shared record in Redis:

    rep:<sensor_id>  hash  r = reputation, v = version, n = number of evidence updates applied,
                           w:<region> = last window applied by that region (makes updates idempotent)

The update is a read-modify-write: read r, compute r' = update_reputation(r, q), write r'.
If two leaders do this at the same time, one update can silently overwrite the other: a LOST UPDATE.

Strategies (CONCURRENCY env):
  none        read, compute, write. No protection: shows the lost-update problem.
  optimistic  WATCH the key, read, compute, then MULTI/EXEC; if anyone changed the key in between,
              EXEC fails and the update is retried. No waiting, but retries under contention.
  lock        take a distributed lock (SET NX PX with a random token), read, compute, write,
              release (only if we still own the lock). Correct, but writers queue behind each other.
  atomic      the whole read-compute-write runs inside Redis as one Lua script. Possible because the
              update is a pure function of (old reputation, score): the computation moves to the data,
              so there is no gap between read and write to race in.

CONCURRENCY_THINK_MS adds a delay between read and write (models processing or network latency)
to widen the race window in experiments; it has no effect on `atomic`.
"""
from __future__ import annotations

import asyncio
import time
import uuid

from prometheus_client import Counter, Histogram
from redis.exceptions import WatchError

from common.trust import TrustParams, update_reputation

STRATEGIES = ("none", "optimistic", "lock", "atomic")

UPDATES = Counter("tm_rep_updates_total", "Shared reputation updates", ["strategy", "result"])
CONFLICTS = Counter("tm_rep_conflicts_total", "Optimistic transactions aborted by a concurrent write", ["strategy"])
LOCK_WAIT = Histogram("tm_rep_lock_wait_seconds", "Time spent waiting for a reputation lock",
                      buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1))
UPDATE_SECONDS = Histogram("tm_rep_update_seconds", "Latency of one shared reputation update", ["strategy"],
                           buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1))

ATOMIC_LUA = """
-- KEYS[1] = rep key; ARGV = score, prior, alpha_up, alpha_down, region, window, weight
local cur = redis.call('HGET', KEYS[1], 'r')
local seen = redis.call('HGET', KEYS[1], 'w:' .. ARGV[5])
if seen and tonumber(seen) >= tonumber(ARGV[6]) then
  return {cur or ARGV[2], 0}
end
local r = tonumber(cur or ARGV[2])
local q = tonumber(ARGV[1])
local a = tonumber(ARGV[3])
if q < r then a = tonumber(ARGV[4]) end
r = r + a * tonumber(ARGV[7]) * (q - r)
local s = string.format('%.10f', r)
redis.call('HSET', KEYS[1], 'r', s, 'w:' .. ARGV[5], ARGV[6])
redis.call('HINCRBY', KEYS[1], 'v', 1)
redis.call('HINCRBY', KEYS[1], 'n', 1)
return {s, 1}
"""

RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def rep_key(sensor_id: str) -> str:
    return f"rep:{sensor_id}"


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class SharedReputation:
    def __init__(self, redis, strategy: str = "optimistic", params: TrustParams | None = None,
                 think_ms: float = 0.0, lock_ttl_ms: int = 2000, max_retries: int = 500):
        if strategy not in STRATEGIES:
            raise ValueError(f"CONCURRENCY must be one of {STRATEGIES}, got {strategy!r}")
        self.redis = redis
        self.strategy = strategy
        self.params = params or TrustParams()
        self.think_s = think_ms / 1000
        self.lock_ttl_ms = lock_ttl_ms
        self.max_retries = max_retries
        self._atomic = redis.register_script(ATOMIC_LUA)
        self._release = redis.register_script(RELEASE_LUA)

    async def read_many(self, sensor_ids: list[str]) -> dict[str, float | None]:
        pipe = self.redis.pipeline(transaction=False)
        for sid in sensor_ids:
            pipe.hget(rep_key(sid), "r")
        values = await pipe.execute()
        return {sid: (float(v) if v is not None else None) for sid, v in zip(sensor_ids, values)}

    def _compute(self, data: dict, score: float, region: str, window: int, prior: float, weight: float = 1.0):
        """New field values, or None if this region already applied this window (idempotency)."""
        fields = {_text(k): _text(v) for k, v in data.items()}
        if int(fields.get(f"w:{region}", -1)) >= window:
            return None, float(fields.get("r", prior))
        rep = float(fields["r"]) if "r" in fields else prior
        new = update_reputation(rep, score, self.params, weight)
        return {"r": f"{new:.10f}", "v": int(fields.get("v", 0)) + 1, "n": int(fields.get("n", 0)) + 1,
                f"w:{region}": window}, new

    async def _think(self) -> None:
        if self.think_s > 0:
            await asyncio.sleep(self.think_s)

    async def apply(self, sensor_id: str, score: float, region: str, window: int,
                    prior: float, weight: float = 1.0) -> tuple[float, bool, int, float]:
        """Apply one region's evidence for one window (as a partial vote of `weight`).
        Returns (new reputation, applied?, retries, lock wait seconds)."""
        started = time.perf_counter()
        key = rep_key(sensor_id)
        retries, wait = 0, 0.0
        args = (key, score, region, window, prior, weight)
        if self.strategy == "atomic":
            result = await self._atomic(keys=[key], args=[repr(score), repr(prior), self.params.alpha_up,
                                                         self.params.alpha_down, region, window, repr(weight)])
            new, applied = float(_text(result[0])), int(result[1]) == 1
        elif self.strategy == "optimistic":
            new, applied, retries = await self._optimistic(*args)
        elif self.strategy == "lock":
            new, applied, wait = await self._locked(*args)
        else:
            new, applied = await self._unprotected(*args)
        UPDATE_SECONDS.labels(self.strategy).observe(time.perf_counter() - started)
        UPDATES.labels(self.strategy, "applied" if applied else "duplicate").inc()
        return new, applied, retries, wait

    async def _unprotected(self, key, score, region, window, prior, weight):
        fields, new = self._compute(await self.redis.hgetall(key), score, region, window, prior, weight)
        if fields is None:
            return new, False
        await self._think()                      # <- another writer can slip in here
        await self.redis.hset(key, mapping=fields)
        return new, True

    async def _optimistic(self, key, score, region, window, prior, weight):
        retries = 0
        async with self.redis.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(key)        # EXEC will fail if the key changes after this
                    fields, new = self._compute(await pipe.hgetall(key), score, region, window, prior, weight)
                    if fields is None:
                        await pipe.unwatch()
                        return new, False, retries
                    await self._think()
                    pipe.multi()
                    pipe.hset(key, mapping=fields)
                    await pipe.execute()
                    return new, True, retries
                except WatchError:
                    retries += 1
                    CONFLICTS.labels(self.strategy).inc()
                    if retries >= self.max_retries:
                        raise

    async def _locked(self, key, score, region, window, prior, weight):
        lock, token = f"lock:{key}", uuid.uuid4().hex
        started = time.perf_counter()
        while not await self.redis.set(lock, token, nx=True, px=self.lock_ttl_ms):
            if time.perf_counter() - started > 5:
                raise TimeoutError(f"could not lock {key} within 5 s")
            await asyncio.sleep(0.001)
        wait = time.perf_counter() - started
        LOCK_WAIT.observe(wait)
        try:
            new, applied = await self._unprotected(key, score, region, window, prior, weight)
        finally:
            await self._release(keys=[lock], args=[token])
        return new, applied, wait
