"""Message formats exchanged over MQTT, with validation."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass

SCHEMA_VERSION = 1


def readings_topic(region: str, cell: str, sensor_id: str) -> str:
    """readings/<region>/<cell>/<sensor>: owners subscribe to readings/<region>/#, neighbours to
    readings/<region>/<cell>/# for the cells along their border."""
    return f"readings/{region}/{cell}/{sensor_id}"


DEADLETTER_TOPIC = "deadletter/readings"


@dataclass(frozen=True, slots=True)
class Reading:
    sensor_id: str
    region: str
    x: float
    y: float
    value: float
    ts: int   # sensor's local clock, unix milliseconds
    seq: int  # per-sensor monotonically increasing sequence number
    v: int = SCHEMA_VERSION

    def to_json(self) -> bytes:
        return json.dumps({
            "v": self.v, "sensor_id": self.sensor_id, "region": self.region,
            "x": self.x, "y": self.y, "value": round(self.value, 3), "unit": "ug/m3",
            "ts": self.ts, "seq": self.seq,
        }, separators=(",", ":")).encode()


def parse_reading(raw: bytes | bytearray | str) -> Reading:
    """Parse and validate a reading. Raises ValueError with a reason if anything is wrong."""
    try:
        data = json.loads(raw)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"not valid JSON ({exc})") from None
    if not isinstance(data, dict):
        raise ValueError("payload is not a JSON object")
    try:
        reading = Reading(
            sensor_id=str(data["sensor_id"]),
            region=str(data["region"]),
            x=float(data["x"]),
            y=float(data["y"]),
            value=float(data["value"]),
            ts=int(data["ts"]),
            seq=int(data["seq"]),
            v=int(data.get("v", SCHEMA_VERSION)),
        )
    except KeyError as exc:
        raise ValueError(f"missing field {exc}") from None
    except (TypeError, ValueError) as exc:
        raise ValueError(f"bad field type ({exc})") from None
    if reading.v != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema version {reading.v}")
    if not all(math.isfinite(n) for n in (reading.x, reading.y, reading.value)):
        raise ValueError("non-finite number")
    if not reading.sensor_id or not reading.region:
        raise ValueError("empty sensor_id or region")
    return reading


# ---------------- alert votes (Stage 5) ----------------

def votes_topic(region: str) -> str:
    return f"votes/{region}"


@dataclass(frozen=True, slots=True)
class Vote:
    """One replica's opinion about one region's window: is the estimate above the alert threshold?"""
    region: str
    window_start: int
    replica: str
    epoch: int
    leader: bool
    estimate: float
    over: bool

    def to_json(self) -> bytes:
        return json.dumps({"v": SCHEMA_VERSION, "region": self.region, "window_start": self.window_start,
                           "replica": self.replica, "epoch": self.epoch, "leader": self.leader,
                           "estimate": round(self.estimate, 3), "over": self.over}, separators=(",", ":")).encode()


def parse_vote(raw: bytes | bytearray | str) -> Vote:
    try:
        d = json.loads(raw)
        vote = Vote(str(d["region"]), int(d["window_start"]), str(d["replica"]), int(d["epoch"]),
                    bool(d["leader"]), float(d["estimate"]), bool(d["over"]))
    except (ValueError, TypeError, KeyError) as exc:
        raise ValueError(f"invalid vote ({exc})") from None
    if not math.isfinite(vote.estimate):
        raise ValueError("non-finite estimate")
    return vote
