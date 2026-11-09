"""TrustMesh trust algorithm: scores every sensor once per window and produces a trust-weighted
regional estimate. Pure computation (NumPy only), no network or database, so it can be unit-tested
and evaluated offline with exactly the code the live aggregator runs.

Per window, for each sensor i with window value x_i:

  Signal 1 - spatial agreement s_i in (0, 1]
      reference m_i  = weighted median of neighbours within `radius_km`,
                       weighted by neighbour reputation and distance
      scale          = robust spread of those neighbours (MAD), floored at `mad_floor`
      s_i            = 1 / (1 + (|x_i - m_i| / scale / k)^2)
      The scale adapts to how much honest neighbours disagree, so honest sensors on a steep
      plume gradient are not punished as hard as an isolated liar.

  Signal 2 - temporal plausibility p_i in (0, 1]
      The sensor's change since its last window must be explained by its neighbourhood's change,
      within `max_unexplained_jump`; a value frozen for `stuck_windows` windows while the
      neighbourhood moves is treated as a stuck sensor.

  Signal 3 - reputation R_i in [0, 1], asymmetric:
      R_i <- R_i + a * (s_i * p_i - R_i),  a = alpha_down if the score is below R_i else alpha_up
      Trust is lost much faster than it is earned, which defeats on-off attackers.
      A flagged sensor is trusted again only when R_i >= unflag_threshold AND it has behaved well for
      `probation_windows` consecutive windows; this stops mild liars flapping in and out of trust.

  Estimate: sensors with R_i below `flag_threshold` are excluded; the rest are averaged with weights
  R_i * min(1, s_i / outlier_gate): a trust-weighted MEAN (unbiased when all sensors are honest,
  see Stage 1) in which only clear outliers of the current window lose weight.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

import numpy as np


@dataclass(frozen=True)
class TrustParams:
    radius_km: float = 1.0
    decay_km: float = 0.6
    k: float = 3.0
    mad_floor: float = 1.5
    min_neighbors: int = 3
    max_unexplained_jump: float = 12.0
    stuck_windows: int = 3
    stuck_neighbor_move: float = 0.3
    alpha_up: float = 0.05
    alpha_down: float = 0.30
    initial_reputation: float = 0.5
    flag_threshold: float = 0.4
    unflag_threshold: float = 0.5  # hysteresis: avoids flapping around the threshold
    probation_windows: int = 12    # consecutive good windows (1 min) before a flagged sensor is trusted again
    good_score: float = 0.6        # a window counts as good when spatial * temporal >= this
    outlier_gate: float = 0.5      # estimate weight = R * min(1, s / gate): only clear outliers lose weight
    isolated_stuck_windows: int = 6  # frozen this long with no neighbours to compare: still treated as stuck


@dataclass
class SensorTrust:
    reputation: float
    last_value: float | None = None
    last_reference: float | None = None
    stuck_count: int = 0
    windows_seen: int = 0
    flagged: bool = False
    reason: str = ""
    good_streak: int = 0


@dataclass
class SensorVerdict:
    sensor_id: str
    value: float
    reference: float | None
    spatial: float
    temporal: float
    reputation: float
    flagged: bool
    reason: str


@dataclass
class WindowResult:
    estimate: float
    confidence: float
    n_sensors: int
    n_trusted: int
    n_flagged: int
    verdicts: list[SensorVerdict] = field(default_factory=list)


def weighted_median(values: np.ndarray, weights: np.ndarray) -> float:
    order = np.argsort(values)
    v, w = values[order], weights[order]
    cumulative = np.cumsum(w)
    idx = int(np.searchsorted(cumulative, cumulative[-1] / 2.0))
    return float(v[min(idx, len(v) - 1)])


class TrustEngine:
    """Keeps per-sensor trust state for one region and processes its windows in order."""

    def __init__(self, params: TrustParams | None = None):
        self.params = params or TrustParams()
        self.states: dict[str, SensorTrust] = {}

    def state(self, sensor_id: str) -> SensorTrust:
        st = self.states.get(sensor_id)
        if st is None:
            st = SensorTrust(reputation=self.params.initial_reputation)
            self.states[sensor_id] = st
        return st

    # ---------------- persistence ----------------
    def export_state(self) -> dict[str, dict]:
        """Plain-dict snapshot of every sensor's trust state (JSON-serialisable)."""
        return {sid: asdict(st) for sid, st in self.states.items()}

    def import_state(self, data: dict[str, dict]) -> None:
        """Replace the current state with a snapshot, ignoring unknown fields from other versions."""
        fields = SensorTrust.__dataclass_fields__
        self.states = {sid: SensorTrust(**{k: v for k, v in d.items() if k in fields}) for sid, d in data.items()}

    # ---------------- signals ----------------
    def _spatial(self, xs, ys, values, reps):
        p = self.params
        n = len(values)
        spatial = np.ones(n)
        refs = np.full(n, np.nan)
        dx = xs[:, None] - xs[None, :]
        dy = ys[:, None] - ys[None, :]
        dist = np.sqrt(dx * dx + dy * dy)
        near = (dist <= p.radius_km) & ~np.eye(n, dtype=bool)
        for i in range(n):
            nb = np.flatnonzero(near[i])
            if len(nb) < p.min_neighbors:
                continue  # not enough evidence: no spatial penalty
            w = reps[nb] * np.exp(-dist[i, nb] / p.decay_km)
            if w.sum() <= 1e-9:
                continue
            ref = weighted_median(values[nb], w)
            mad = weighted_median(np.abs(values[nb] - ref), w)
            scale = max(1.4826 * mad, p.mad_floor)
            z = abs(values[i] - ref) / scale
            spatial[i] = 1.0 / (1.0 + (z / p.k) ** 2)
            refs[i] = ref
        return spatial, refs

    def _temporal(self, st: SensorTrust, value: float, ref: float) -> tuple[float, str]:
        p = self.params
        if st.last_value is None:
            return 1.0, ""
        change = value - st.last_value
        ref_change = 0.0
        if st.last_reference is not None and not np.isnan(ref):
            ref_change = ref - st.last_reference
        score, reason = 1.0, ""
        unexplained = abs(change - ref_change)
        if unexplained > p.max_unexplained_jump:
            score, reason = (p.max_unexplained_jump / unexplained) ** 2, "implausible jump"
        st.stuck_count = st.stuck_count + 1 if abs(change) < 1e-6 else 0
        if st.stuck_count >= p.stuck_windows and abs(ref_change) > p.stuck_neighbor_move:
            score, reason = min(score, 0.1), "stuck value"
        elif np.isnan(ref) and st.stuck_count >= p.isolated_stuck_windows:
            # No neighbours to compare with, but honest noisy readings never repeat exactly.
            score, reason = min(score, 0.1), "stuck value"
        return score, reason

    # ---------------- main entry ----------------
    def process_window(self, sensor_ids: list[str], xs, ys, values) -> WindowResult:
        p = self.params
        xs, ys, values = (np.asarray(a, dtype=float) for a in (xs, ys, values))
        n = len(values)
        if n == 0:
            return WindowResult(float("nan"), 0.0, 0, 0, 0)
        states = [self.state(sid) for sid in sensor_ids]
        reps = np.array([st.reputation for st in states])
        spatial, refs = self._spatial(xs, ys, values, reps)

        verdicts = []
        for i, (sid, st) in enumerate(zip(sensor_ids, states)):
            temporal, t_reason = self._temporal(st, values[i], refs[i])
            score = spatial[i] * temporal
            alpha = p.alpha_down if score < st.reputation else p.alpha_up
            st.reputation += alpha * (score - st.reputation)
            st.good_streak = st.good_streak + 1 if score >= p.good_score else 0
            if st.flagged and st.reputation >= p.unflag_threshold and st.good_streak >= p.probation_windows:
                st.flagged = False
            elif not st.flagged and st.reputation < p.flag_threshold:
                st.flagged = True
            if st.flagged:
                st.reason = t_reason or ("spatial outlier" if spatial[i] < 0.5 else st.reason or "low reputation")
            else:
                st.reason = ""
            st.last_value = float(values[i])
            st.last_reference = None if np.isnan(refs[i]) else float(refs[i])
            st.windows_seen += 1
            verdicts.append(SensorVerdict(sid, float(values[i]), st.last_reference, float(spatial[i]),
                                          float(temporal), float(st.reputation), st.flagged, st.reason))

        trusted = np.array([not st.flagged for st in states])
        gate = np.minimum(1.0, spatial / p.outlier_gate) if p.outlier_gate > 0 else spatial
        weights = np.array([st.reputation for st in states]) * gate * trusted
        if weights.sum() > 1e-9:
            estimate = float(np.sum(weights * values) / weights.sum())
        else:
            estimate = float(np.median(values))  # nobody trusted: fall back, confidence 0
        confidence = float(np.sum(reps * trusted) / n)
        return WindowResult(estimate, confidence, n, int(trusted.sum()), int(n - trusted.sum()), verdicts)
