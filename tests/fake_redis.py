"""Minimal in-memory stand-in for redis.asyncio, with the semantics the reputation strategies rely on:
WATCH/MULTI/EXEC aborts with WatchError if a watched key changed, SET NX for locks, and Python
equivalents of the two Lua scripts. Every call yields to the event loop, so concurrent tasks interleave
like real network round trips. The real Redis is exercised by experiments/concurrency_bench.py."""
import asyncio
from collections import defaultdict

from redis.exceptions import WatchError


def _b(x):
    return x if isinstance(x, bytes) else str(x).encode()


class FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}
        self.strings: dict[str, str] = {}
        self.version: dict[str, int] = defaultdict(int)

    async def _rt(self):
        await asyncio.sleep(0)

    async def hgetall(self, key):
        await self._rt()
        return {_b(k): _b(v) for k, v in self.hashes.get(key, {}).items()}

    async def hget(self, key, field):
        await self._rt()
        v = self.hashes.get(key, {}).get(field)
        return None if v is None else _b(v)

    def _hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update({k: str(v) for k, v in mapping.items()})
        self.version[key] += 1

    async def hset(self, key, mapping):
        await self._rt()
        self._hset(key, mapping)

    async def set(self, key, value, nx=False, px=None):
        await self._rt()
        if nx and key in self.strings:
            return None
        self.strings[key] = str(value)
        self.version[key] += 1
        return True

    def register_script(self, source):
        return _Script(self, source)

    def pipeline(self, transaction=True):
        return _Pipeline(self)


class _Script:
    def __init__(self, redis, source):
        self.redis, self.source = redis, source

    async def __call__(self, keys, args):
        await self.redis._rt()
        if "HINCRBY" in self.source:            # ATOMIC_LUA, executed without yielding (atomic)
            h = self.redis.hashes.setdefault(keys[0], {})
            score, prior, a_up, a_down, region, window, weight = args
            cur = h.get("r")
            seen = h.get(f"w:{region}")
            if seen is not None and int(seen) >= int(window):
                return [_b(cur or prior), 0]
            r = float(cur if cur is not None else prior)
            q = float(score)
            a = float(a_down) if q < r else float(a_up)
            r = r + a * float(weight) * (q - r)
            s = f"{r:.10f}"
            h.update({"r": s, f"w:{region}": str(window)})
            h["v"] = str(int(h.get("v", 0)) + 1)
            h["n"] = str(int(h.get("n", 0)) + 1)
            self.redis.version[keys[0]] += 1
            return [_b(s), 1]
        if "DEL" in self.source:                 # RELEASE_LUA
            if self.redis.strings.get(keys[0]) == str(args[0]):
                del self.redis.strings[keys[0]]
                return 1
            return 0
        raise NotImplementedError(self.source)


class _Pipeline:
    def __init__(self, redis):
        self.redis = redis
        self.watched: dict[str, int] = {}
        self.queued: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.watched.clear()

    async def watch(self, key):
        await self.redis._rt()
        self.watched[key] = self.redis.version[key]

    async def unwatch(self):
        self.watched.clear()

    async def hgetall(self, key):
        return await self.redis.hgetall(key)

    def multi(self):
        self.queued = []

    def hset(self, key, mapping):
        self.queued.append(("hset", key, mapping))
        return self

    def hget(self, key, field):
        self.queued.append(("hget", key, field))
        return self

    async def execute(self):
        await self.redis._rt()
        if any(self.redis.version[k] != v for k, v in self.watched.items()):
            self.watched.clear()
            self.queued = []
            raise WatchError("watched key changed")
        out = []
        for op, key, arg in self.queued:
            if op == "hset":
                self.redis._hset(key, arg)
                out.append(1)
            else:
                v = self.redis.hashes.get(key, {}).get(arg)
                out.append(None if v is None else _b(v))
        self.queued, self.watched = [], {}
        return out
