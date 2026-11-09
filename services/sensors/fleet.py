"""Builds the simulated sensor fleet: deterministic positions and behaviors from the seed."""
from __future__ import annotations

import random
from dataclasses import dataclass, replace

from common.config import Config
from common.geo import region_of, to_latlon
from services.sensors.behaviors import parse_mix


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


def build_fleet(cfg: Config, behavior_mix: str | None = None) -> list[SensorSpec]:
    """30% of sensors cluster around a dense downtown, the rest spread uniformly.
    `behavior_mix` (default: cfg.behavior_mix), e.g. 'offset_high:0.1,stuck:0.05', turns that
    fraction of sensors faulty; which sensors is chosen deterministically from the seed."""
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
        fleet.append(SensorSpec(f"s-{i:04d}", i, x, y, region_of(x, y, cfg), lat, lon, "honest"))

    mix = parse_mix(cfg.behavior_mix if behavior_mix is None else behavior_mix)
    order = list(range(len(fleet)))
    random.Random(cfg.seed + 1).shuffle(order)
    cursor = 0
    for name, fraction in mix.items():
        count = round(fraction * len(fleet))
        for idx in order[cursor:cursor + count]:
            fleet[idx] = replace(fleet[idx], behavior=name)
        cursor += count
    return fleet
