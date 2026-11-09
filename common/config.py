"""Configuration shared by every TrustMesh service and script, read from environment variables.

The simulated-city settings (seed, city size, grid, day length, plumes) MUST be identical in every
service and in the experiment scripts, otherwise they disagree about the ground truth. Leave them
at their defaults unless you change them everywhere.
"""
from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _bool(raw: str) -> bool:
    value = raw.strip().lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    raise ValueError(raw)


def _get(name: str, default, cast=str):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name}={raw!r} is not a valid {cast.__name__}") from exc


@dataclass(frozen=True)
class Config:
    service_name: str
    instance_id: str
    # Messaging
    mqtt_host: str
    mqtt_port: int
    mqtt_qos: int
    # Database
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_db: str
    # Coordination (leases, shared trust state)
    redis_host: str
    redis_port: int
    lease_ttl_ms: int
    lease_renew_ms: int
    fencing_self_check: bool
    # Observability
    log_url: str
    metrics_port: int
    # Simulated city and ground truth (shared by everything)
    seed: int
    city_w_km: float
    city_h_km: float
    grid_rows: int
    grid_cols: int
    origin_lat: float
    origin_lon: float
    day_seconds: float
    num_plumes: int
    # Sensors
    num_sensors: int
    publish_interval_s: float
    noise_std: float
    behavior_mix: str
    # Aggregation
    regions: str
    cell_km: float
    border_exchange: bool
    concurrency: str
    concurrency_think_ms: float
    # Alerting (Stage 5)
    alert_threshold: float
    alert_quorum: int
    alert_replicas: int
    alert_clear_windows: int
    vote_bias: float
    window_ms: int
    grace_ms: int
    db_flush_interval_s: float

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password} connect_timeout=5"
        )

    @property
    def region_filter(self) -> set[str] | None:
        """None means 'all regions'."""
        if self.regions.strip().lower() in ("", "all", "*"):
            return None
        return {r.strip() for r in self.regions.split(",") if r.strip()}


def load_config(service_name: str) -> Config:
    return Config(
        service_name=_get("SERVICE_NAME", service_name),
        instance_id=_get("INSTANCE_ID", socket.gethostname()),
        mqtt_host=_get("MQTT_HOST", "mosquitto"),
        mqtt_port=_get("MQTT_PORT", 1883, int),
        mqtt_qos=_get("MQTT_QOS", 1, int),
        pg_host=_get("PG_HOST", "postgres"),
        pg_port=_get("PG_PORT", 5432, int),
        pg_user=_get("POSTGRES_USER", "trustmesh"),
        pg_password=_get("POSTGRES_PASSWORD", "trustmesh"),
        pg_db=_get("POSTGRES_DB", "trustmesh"),
        redis_host=_get("REDIS_HOST", "redis"),
        redis_port=_get("REDIS_PORT", 6379, int),
        lease_ttl_ms=_get("LEASE_TTL_MS", 3000, int),
        lease_renew_ms=_get("LEASE_RENEW_MS", 1000, int),
        fencing_self_check=_get("FENCING_SELF_CHECK", True, _bool),
        log_url=_get("LOG_URL", "http://logging-svc:8000/logs"),
        metrics_port=_get("METRICS_PORT", 8000, int),
        seed=_get("SEED", 42, int),
        city_w_km=_get("CITY_W_KM", 10.0, float),
        city_h_km=_get("CITY_H_KM", 10.0, float),
        grid_rows=_get("GRID_ROWS", 2, int),
        grid_cols=_get("GRID_COLS", 2, int),
        origin_lat=_get("ORIGIN_LAT", 33.85, float),
        origin_lon=_get("ORIGIN_LON", 35.48, float),
        day_seconds=_get("DAY_SECONDS", 900.0, float),
        num_plumes=_get("NUM_PLUMES", 3, int),
        num_sensors=_get("NUM_SENSORS", 400, int),
        publish_interval_s=_get("PUBLISH_INTERVAL_S", 2.0, float),
        noise_std=_get("NOISE_STD", 2.0, float),
        behavior_mix=_get("BEHAVIOR_MIX", ""),
        regions=_get("REGIONS", "all"),
        cell_km=_get("CELL_KM", 1.0, float),
        border_exchange=_get("BORDER_EXCHANGE", True, _bool),
        concurrency=_get("CONCURRENCY", "optimistic"),
        concurrency_think_ms=_get("CONCURRENCY_THINK_MS", 0.0, float),
        alert_threshold=_get("ALERT_THRESHOLD", 35.5, float),
        alert_quorum=_get("ALERT_QUORUM", 2, int),
        alert_replicas=_get("ALERT_REPLICAS", 2, int),
        alert_clear_windows=_get("ALERT_CLEAR_WINDOWS", 2, int),
        vote_bias=_get("VOTE_BIAS", 0.0, float),
        window_ms=_get("WINDOW_MS", 5000, int),
        grace_ms=_get("GRACE_MS", 2000, int),
        db_flush_interval_s=_get("DB_FLUSH_INTERVAL_S", 1.0, float),
    )
