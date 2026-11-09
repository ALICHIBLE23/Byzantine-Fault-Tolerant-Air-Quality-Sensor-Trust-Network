"""Sensor behaviors: one honest profile and the faulty / malicious profiles TrustMesh must detect.

Every behavior gets the true value at its position and returns what the sensor reports
(or None to stay silent). Behaviors are deterministic given their random generator, so
experiments are repeatable.
"""
from __future__ import annotations

import random

from common.config import Config


class Behavior:
    name = "base"
    faulty = True

    def __init__(self, noise_std: float, rng: random.Random, start_t: float):
        self.noise_std = noise_std
        self.start_t = start_t

    def noisy(self, value: float, rng: random.Random) -> float:
        return max(0.0, value + rng.gauss(0.0, self.noise_std))

    def observe(self, true_value: float, t: float, rng: random.Random) -> float | None:
        raise NotImplementedError


class Honest(Behavior):
    name, faulty = "honest", False

    def observe(self, true_value, t, rng):
        return self.noisy(true_value, rng)


class Stuck(Behavior):
    """Hardware fault: works for a while, then freezes on one value forever."""
    name = "stuck"

    def __init__(self, noise_std, rng, start_t):
        super().__init__(noise_std, rng, start_t)
        self.fail_after = rng.uniform(20, 90)
        self.frozen: float | None = None

    def observe(self, true_value, t, rng):
        if t - self.start_t < self.fail_after:
            return self.noisy(true_value, rng)
        if self.frozen is None:
            self.frozen = round(self.noisy(true_value, rng), 3)
        return self.frozen


class Drift(Behavior):
    """Ageing sensor: a bias that grows slowly over time (the hardest fault to notice)."""
    name = "drift"

    def __init__(self, noise_std, rng, start_t):
        super().__init__(noise_std, rng, start_t)
        self.rate = rng.uniform(0.03, 0.08) * rng.choice((1, -1))  # ug/m3 per second

    def observe(self, true_value, t, rng):
        return self.noisy(true_value + self.rate * (t - self.start_t), rng)


class Spike(Behavior):
    """Interference: mostly fine, but frequent large random spikes."""
    name = "spike"

    def observe(self, true_value, t, rng):
        if rng.random() < 0.25:
            return self.noisy(true_value + rng.uniform(30, 90), rng)
        return self.noisy(true_value, rng)


class OffsetHigh(Behavior):
    """Malicious: inflates readings to trigger false pollution alarms."""
    name = "offset_high"

    def __init__(self, noise_std, rng, start_t):
        super().__init__(noise_std, rng, start_t)
        self.offset = rng.uniform(15, 35)

    def observe(self, true_value, t, rng):
        return self.noisy(true_value + self.offset, rng)


class Hider(Behavior):
    """Malicious: under-reports to hide real pollution (e.g. near a polluter)."""
    name = "hider"

    def __init__(self, noise_std, rng, start_t):
        super().__init__(noise_std, rng, start_t)
        self.factor = rng.uniform(0.2, 0.5)

    def observe(self, true_value, t, rng):
        return self.noisy(true_value * self.factor, rng)


class OnOff(Behavior):
    """Malicious: behaves honestly to build reputation, then lies in bursts."""
    name = "on_off"

    def __init__(self, noise_std, rng, start_t):
        super().__init__(noise_std, rng, start_t)
        self.honest_for = rng.uniform(60, 120)
        self.offset = rng.uniform(20, 35)
        self.period = 60.0

    def observe(self, true_value, t, rng):
        elapsed = t - self.start_t - self.honest_for
        if elapsed < 0 or (elapsed // self.period) % 2 == 1:
            return self.noisy(true_value, rng)
        return self.noisy(true_value + self.offset, rng)


BEHAVIORS: dict[str, type[Behavior]] = {
    b.name: b for b in (Honest, Stuck, Drift, Spike, OffsetHigh, Hider, OnOff)
}


def make_behavior(name: str, cfg: Config, rng: random.Random, start_t: float) -> Behavior:
    try:
        return BEHAVIORS[name](cfg.noise_std, rng, start_t)
    except KeyError:
        raise ValueError(f"unknown behavior {name!r}; choose from {sorted(BEHAVIORS)}") from None


def parse_mix(spec: str) -> dict[str, float]:
    """'offset_high:0.1,stuck:0.05' -> {'offset_high': 0.1, 'stuck': 0.05}"""
    mix: dict[str, float] = {}
    for part in filter(None, (p.strip() for p in spec.split(","))):
        name, _, frac = part.partition(":")
        if name not in BEHAVIORS or name == "honest":
            raise ValueError(f"unknown faulty behavior {name!r}; choose from {sorted(set(BEHAVIORS) - {'honest'})}")
        mix[name] = float(frac)
    if sum(mix.values()) > 1.0:
        raise ValueError(f"behavior fractions add up to more than 1: {spec}")
    return mix
