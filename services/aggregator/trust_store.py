"""Shared trust state in Redis, so reputations survive crashes and move with leadership.

trust:<region> is a hash: sensor_id -> JSON of that sensor's trust state.
Saving is fenced by lease ownership: a Lua script writes only if the caller still holds
lease:<region>, so a stale leader cannot overwrite the new leader's state.
"""
from __future__ import annotations

import json

from services.aggregator.leases import lease_key

SAVE_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 0
end
for i = 2, #ARGV, 2 do
  redis.call('HSET', KEYS[2], ARGV[i], ARGV[i + 1])
end
return 1
"""


def trust_key(region: str) -> str:
    return f"trust:{region}"


class RedisTrustStore:
    def __init__(self, redis):
        self.redis = redis
        self._save = redis.register_script(SAVE_LUA)

    async def load(self, region: str) -> dict[str, dict]:
        raw = await self.redis.hgetall(trust_key(region))
        return {(k.decode() if isinstance(k, bytes) else k): json.loads(v) for k, v in raw.items()}

    async def save(self, region: str, owner: str, states: dict[str, dict]) -> bool:
        args: list[str] = [owner]
        for sensor_id, state in states.items():
            args += [sensor_id, json.dumps(state, separators=(",", ":"))]
        return bool(await self._save(keys=[lease_key(region), trust_key(region)], args=args))
