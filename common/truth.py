"""Ground-truth pollution field (PM2.5-like, ug/m3).

A pure, deterministic function of position and time: every service and script that calls it with
the same configuration gets the same value, so the field never has to be sent over the network.
Aggregators must never call it; only sensors (to take measurements) and evaluation scripts
(to score estimates) do.

field = background with a compressed daily cycle
      + a fixed smooth spatial texture
      + a few moving, pulsing Gaussian plumes (factories, traffic hot spots)
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from functools import lru_cache

from common.config import Config


@dataclass(frozen=True)
class Plume:
    x0: float
    y0: float
    orbit_km: float
    omega: float
    phase: float
    amplitude: float
    sigma_km: float
    pulse_period_s: float
    pulse_phase: float


@lru_cache(maxsize=16)
def plumes_for(seed: int, width_km: float, height_km: float, count: int) -> tuple[Plume, ...]:
    rng = random.Random(seed * 7919 + 17)
    plumes = []
    for _ in range(count):
        plumes.append(
            Plume(
                x0=rng.uniform(0.15, 0.85) * width_km,
                y0=rng.uniform(0.15, 0.85) * height_km,
                orbit_km=rng.uniform(0.3, 1.5),
                omega=2 * math.pi / rng.uniform(300, 900),
                phase=rng.uniform(0, 2 * math.pi),
                amplitude=rng.uniform(20, 55),
                sigma_km=rng.uniform(0.6, 1.4),
                pulse_period_s=rng.uniform(120, 480),
                pulse_phase=rng.uniform(0, 2 * math.pi),
            )
        )
    return tuple(plumes)


def truth_at(x: float, y: float, t: float, *, seed: int, width_km: float, height_km: float,
             day_seconds: float, num_plumes: int) -> float:
    background = 18.0 + 6.0 * math.sin(2 * math.pi * t / day_seconds)
    texture = 3.0 + 3.0 * math.sin(0.9 * x + 0.6) * math.cos(0.7 * y - 0.4)
    total = background + texture
    for p in plumes_for(seed, width_km, height_km, num_plumes):
        cx = p.x0 + p.orbit_km * math.cos(p.omega * t + p.phase)
        cy = p.y0 + p.orbit_km * math.sin(p.omega * t + p.phase)
        strength = p.amplitude * (0.6 + 0.4 * math.sin(2 * math.pi * t / p.pulse_period_s + p.pulse_phase))
        d2 = (x - cx) ** 2 + (y - cy) ** 2
        total += strength * math.exp(-d2 / (2 * p.sigma_km ** 2))
    return max(0.0, total)


def truth(x: float, y: float, t: float, cfg: Config) -> float:
    """True value at position (x, y) km and unix time t (seconds)."""
    return truth_at(x, y, t, seed=cfg.seed, width_km=cfg.city_w_km, height_km=cfg.city_h_km,
                    day_seconds=cfg.day_seconds, num_plumes=cfg.num_plumes)
