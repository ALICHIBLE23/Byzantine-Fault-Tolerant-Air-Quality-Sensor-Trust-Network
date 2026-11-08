"""Baseline aggregation methods. Stage 2 adds the TrustMesh trust-weighted method next to these,
so every window is scored by all methods on identical data (the core comparison experiment)."""
from __future__ import annotations

import numpy as np


def mean(values: np.ndarray) -> float:
    return float(np.mean(values))


def median(values: np.ndarray) -> float:
    return float(np.median(values))


def trimmed_mean(values: np.ndarray, proportion: float = 0.2) -> float:
    """Drop the lowest and highest `proportion` of values, average the rest."""
    s = np.sort(values)
    k = int(len(s) * proportion)
    if len(s) - 2 * k < 1:
        return float(np.median(s))
    return float(np.mean(s[k:len(s) - k]))


ALGORITHMS = {
    "mean": mean,
    "median": median,
    "trimmed_mean": trimmed_mean,
}
