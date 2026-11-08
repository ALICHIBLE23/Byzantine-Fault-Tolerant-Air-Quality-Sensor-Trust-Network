"""Builds the simulated sensor fleet: deterministic positions from the seed."""
from __future__ import annotations

import random
from dataclasses import dataclass

from common.config import Config
from common.geo import region_of, to_latlon


@dataclass(frozen=True)
class SensorSpec:
    sensor_id: str
    index: int
    x_km: float
    y_km: float
    region: str
    lat: float
    lon: float
    behavior: str


def build_fleet(cfg: Config) -> list[SensorSpec]:
    """30% of sensors cluster around a dense downtown, the rest spread uniformly."""
    rng = random.Random(cfg.seed)
    w, h = cfg.city_w_km, cfg.city_h_km
    downtown_x, downtown_y = 0.5 * w, 0.55 * h
    fleet = []
    for i in range(cfg.num_sensors):
        if rng.random() < 0.3:
            x = rng.gauss(downtown_x, 1.2)
            y = rng.gauss(downtown_y, 1.2)
        else:
            x = rng.uniform(0, w)
            y = rng.uniform(0, h)
        x = round(min(max(x, 0.0), w - 1e-6), 4)
        y = round(min(max(y, 0.0), h - 1e-6), 4)
        lat, lon = to_latlon(x, y, cfg)
        fleet.append(SensorSpec(
            sensor_id=f"s-{i:04d}", index=i, x_km=x, y_km=y,
            region=region_of(x, y, cfg), lat=lat, lon=lon,
            behavior="honest",  # Stage 5 assigns faulty and malicious behaviors here
        ))
    return fleet
