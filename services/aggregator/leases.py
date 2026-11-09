"""Leader election with leases and fencing epochs.

Every region has one lease key in Redis:  lease:<region> = <replica id>, with a time-to-live.
  * A replica acquires a free lease with SET NX PX (atomic: only one replica can win).
  * The leader renews it every `renew_interval_s`; the renewal is a Lua script that only extends
    the lease if the caller still owns it, so a replica can never extend someone else's lease.
  * If the leader crashes, the lease expires after `ttl_ms` and a standby takes it.
  * Each acquisition increments epoch:<region>. The epoch is a fencing token: every write a leader
    makes carries it, and the database rejects writes whose epoch is older than the newest one.

Two layers protect against a stale leader (for example a process that was paused and wakes up):
  1. self-check: a replica stops considering itself leader once its last successful renewal is
     older than 80% of the TTL, even before it learns that someone else took over;
  2. fencing: even if layer 1 is bypassed (FENCING_SELF_CHECK=false), the database rejects its
     writes because a newer epoch has been published.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def lease_key(region: str) -> str:
    return f"lease:{region}"


def epoch_key(region: str) -> str:
    return f"epoch:{region}"


class LeaseBackend(Protocol):
    async def try_acquire(self, region: str, owner: str, ttl_ms: int) -> bool: ...
    async def renew(self, region: str, owner: str, ttl_ms: int) -> bool: ...
    async def release(self, region: str, owner: str) -> bool: ...
    async def next_epoch(self, region: str) -> int: ...


class RedisLeaseBackend:
    def __init__(self, redis):
        self.redis = redis
        self._renew = redis.register_script(RENEW_LUA)
        self._release = redis.register_script(RELEASE_LUA)

    async def try_acquire(self, region, owner, ttl_ms):
        return bool(await self.redis.set(lease_key(region), owner, nx=True, px=ttl_ms))

    async def renew(self, region, owner, ttl_ms):
        return bool(await self._renew(keys=[lease_key(region)], args=[owner, ttl_ms]))

    async def release(self, region, owner):
        return bool(await self._release(keys=[lease_key(region)], args=[owner]))

    async def next_epoch(self, region):
        return int(await self.redis.incr(epoch_key(region)))


class InMemoryLeaseBackend:
    """Same semantics as RedisLeaseBackend, for unit tests (time comes from `clock`)."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self.leases: dict[str, tuple[str, float]] = {}
        self.epochs: dict[str, int] = defaultdict(int)

    def holder(self, region: str) -> str | None:
        entry = self.leases.get(region)
        if entry and entry[1] > self.clock():
            return entry[0]
        return None

    async def try_acquire(self, region, owner, ttl_ms):
        if self.holder(region) is not None:
            return False
        self.leases[region] = (owner, self.clock() + ttl_ms / 1000)
        return True

    async def renew(self, region, owner, ttl_ms):
        if self.holder(region) != owner:
            return False
        self.leases[region] = (owner, self.clock() + ttl_ms / 1000)
        return True

    async def release(self, region, owner):
        if self.holder(region) != owner:
            return False
        del self.leases[region]
        return True

    async def next_epoch(self, region):
        self.epochs[region] += 1
        return self.epochs[region]


@dataclass
class Lease:
    epoch: int
    renewed_at: float


Callback = Callable[..., Awaitable[None]]


class LeaseManager:
    def __init__(self, backend: LeaseBackend, regions: list[str], owner: str, *, ttl_ms: int = 3000,
                 renew_interval_s: float = 1.0, self_check: bool = True, log=None,
                 clock: Callable[[], float] = time.monotonic):
        self.backend = backend
        self.regions = list(regions)
        self.owner = owner
        self.ttl_ms = ttl_ms
        self.renew_interval_s = renew_interval_s
        self.self_check = self_check
        self.log = log
        self.clock = clock
        self.held: dict[str, Lease] = {}
        self.on_promote: Callback | None = None   # async (region, epoch)
        self.on_demote: Callback | None = None    # async (region, reason)
        self._errors = 0

    def is_leader(self, region: str) -> bool:
        lease = self.held.get(region)
        if lease is None:
            return False
        if self.self_check and (self.clock() - lease.renewed_at) * 1000 > self.ttl_ms * 0.8:
            return False  # our lease may already have expired: stop acting as leader
        return True

    def epoch(self, region: str) -> int:
        lease = self.held.get(region)
        return lease.epoch if lease else 0

    async def _promote(self, region: str, epoch: int) -> None:
        self.held[region] = Lease(epoch, self.clock())
        if self.log:
            self.log.warning("acquired leader lease for %s (epoch %d)", region, epoch)
        if self.on_promote:
            try:
                await self.on_promote(region, epoch)
            except Exception:
                if self.log:
                    self.log.exception("promotion handler failed for %s", region)

    async def _demote(self, region: str, reason: str) -> None:
        self.held.pop(region, None)
        if self.log:
            self.log.warning("lost leader lease for %s: %s", region, reason)
        if self.on_demote:
            try:
                await self.on_demote(region, reason)
            except Exception:
                if self.log:
                    self.log.exception("demotion handler failed for %s", region)

    async def tick(self) -> None:
        for region in self.regions:
            try:
                if region in self.held:
                    if await self.backend.renew(region, self.owner, self.ttl_ms):
                        self.held[region].renewed_at = self.clock()
                    else:
                        await self._demote(region, "lease expired or taken by another replica")
                elif await self.backend.try_acquire(region, self.owner, self.ttl_ms):
                    await self._promote(region, await self.backend.next_epoch(region))
                self._errors = 0
            except Exception as exc:  # lease store unreachable
                self._errors += 1
                if self.log and (self._errors <= 3 or self._errors % 30 == 0):
                    self.log.warning("lease store error for %s (%s)", region, exc)
                lease = self.held.get(region)
                if lease and (self.clock() - lease.renewed_at) * 1000 > self.ttl_ms:
                    await self._demote(region, "cannot reach the lease store")

    async def run(self) -> None:
        import asyncio
        while True:
            await self.tick()
            await asyncio.sleep(self.renew_interval_s)

    async def release_all(self) -> None:
        """Graceful shutdown: give leases back so a standby takes over immediately."""
        for region in list(self.held):
            try:
                await self.backend.release(region, self.owner)
            except Exception:
                pass
            self.held.pop(region, None)


class StaticLeases:
    """Always leader of every region (single-instance runs and unit tests)."""
    self_check = True

    def __init__(self, epoch: int = 1):
        self._epoch = epoch

    def is_leader(self, region: str) -> bool:
        return True

    def epoch(self, region: str) -> int:
        return self._epoch
