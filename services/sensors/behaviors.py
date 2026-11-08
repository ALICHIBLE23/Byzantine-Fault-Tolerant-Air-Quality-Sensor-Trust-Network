"""Sensor behaviors. Stage 1 has only honest sensors; Stage 5 adds stuck, drifting, noisy,
malicious, on-off and colluding behaviors as more classes in this file."""
from __future__ import annotations

import random

from common.config import Config


class Behavior:
    name = "base"

    def observe(self, true_value: float, t: float, rng: random.Random) -> float | None:
        """Return the value the sensor reports, or None to stay silent this round."""
        raise NotImplementedError


class Honest(Behavior):
    name = "honest"

    def __init__(self, noise_std: float):
        self.noise_std = noise_std

    def observe(self, true_value: float, t: float, rng: random.Random) -> float | None:
        return max(0.0, true_value + rng.gauss(0.0, self.noise_std))


def make_behavior(name: str, cfg: Config) -> Behavior:
    if name == "honest":
        return Honest(cfg.noise_std)
    raise ValueError(f"unknown behavior {name!r}")
