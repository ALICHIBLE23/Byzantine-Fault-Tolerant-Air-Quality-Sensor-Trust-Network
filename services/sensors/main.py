"""Sensor fleet simulator: runs every sensor as an asyncio task inside one process.

Each sensor samples the ground truth at its position, applies its behavior (noise, faults, lies),
and publishes a JSON reading to readings/<region>/<sensor_id> on the MQTT broker.
"""
from __future__ import annotations

import asyncio
import random
import time

import aiomqtt
import psycopg
from prometheus_client import Counter, Gauge, start_http_server

from common.config import Config, load_config
from common.logclient import mqtt_logger, setup_logging
from common.schemas import Reading, readings_topic
from common.truth import truth
from services.sensors.behaviors import Behavior, make_behavior
from services.sensors.fleet import SensorSpec, build_fleet

PUBLISHED = Counter("tm_sensor_published_total", "Readings published", ["region"])
PUBLISH_ERRORS = Counter("tm_sensor_publish_errors_total", "Publish attempts that failed")
ACTIVE = Gauge("tm_sensor_active", "Simulated sensors running")
CONNECTED = Gauge("tm_sensor_mqtt_connected", "1 when connected to the broker")

UPSERT_SENSOR = """
INSERT INTO sensors (sensor_id, region, x_km, y_km, lat, lon, behavior)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (sensor_id) DO UPDATE SET
  region = EXCLUDED.region, x_km = EXCLUDED.x_km, y_km = EXCLUDED.y_km,
  lat = EXCLUDED.lat, lon = EXCLUDED.lon, behavior = EXCLUDED.behavior,
  registered_at = now()
"""


class SimSensor:
    def __init__(self, spec: SensorSpec, behavior: Behavior, seed: int, seq_start: int):
        self.spec = spec
        self.behavior = behavior
        self.rng = random.Random(seed * 1_000_003 + spec.index)
        # Starting the sequence at the boot time in ms keeps it increasing across restarts,
        # so (sensor_id, seq) stays unique and deduplication keeps working.
        self.seq = seq_start
        self.topic = readings_topic(spec.region, spec.sensor_id)


async def register_fleet(cfg: Config, fleet: list[SensorSpec], log) -> None:
    """Write the sensor registry to the database, retrying until it is reachable."""
    rows = [(s.sensor_id, s.region, s.x_km, s.y_km, s.lat, s.lon, s.behavior) for s in fleet]
    ids = [s.sensor_id for s in fleet]
    backoff = 1.0
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(cfg.pg_dsn, autocommit=True) as conn:
                async with conn.cursor() as cur:
                    await cur.executemany(UPSERT_SENSOR, rows)
                    await cur.execute("DELETE FROM sensors WHERE NOT (sensor_id = ANY(%s))", (ids,))
            log.info("registered %d sensors in the database", len(rows))
            return
        except psycopg.Error as exc:
            log.warning("sensor registry write failed (%s); retrying in %.0fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


async def sensor_loop(sensor: SimSensor, client: aiomqtt.Client, cfg: Config) -> None:
    # Random start offset spreads publishes evenly instead of all sensors firing together.
    await asyncio.sleep(sensor.rng.uniform(0, cfg.publish_interval_s))
    spec = sensor.spec
    while True:
        t = time.time()
        value = sensor.behavior.observe(truth(spec.x_km, spec.y_km, t, cfg), t, sensor.rng)
        if value is not None:
            reading = Reading(sensor_id=spec.sensor_id, region=spec.region, x=spec.x_km, y=spec.y_km,
                              value=value, ts=int(t * 1000), seq=sensor.seq)
            try:
                await client.publish(sensor.topic, reading.to_json(), qos=cfg.mqtt_qos)
            except aiomqtt.MqttError:
                PUBLISH_ERRORS.inc()
                raise
            PUBLISHED.labels(spec.region).inc()
            sensor.seq += 1
        await asyncio.sleep(cfg.publish_interval_s)


async def main() -> None:
    cfg = load_config("sensors")
    log = setup_logging(cfg)
    start_http_server(cfg.metrics_port)

    fleet = build_fleet(cfg)
    seq_start = time.time_ns() // 1_000_000
    sensors = [SimSensor(s, make_behavior(s.behavior, cfg), cfg.seed, seq_start) for s in fleet]
    ACTIVE.set(len(sensors))
    regions = sorted({s.region for s in fleet})
    log.info("built fleet of %d sensors across %d regions %s", len(sensors), len(regions), regions)

    await register_fleet(cfg, fleet, log)

    backoff = 1.0
    while True:
        try:
            async with aiomqtt.Client(cfg.mqtt_host, cfg.mqtt_port,
                                      identifier=f"sensors-{cfg.instance_id}",
                                      logger=mqtt_logger()) as client:
                CONNECTED.set(1)
                backoff = 1.0
                log.info("connected to broker %s:%d; publishing every %.1fs",
                         cfg.mqtt_host, cfg.mqtt_port, cfg.publish_interval_s)
                async with asyncio.TaskGroup() as tg:
                    for sensor in sensors:
                        tg.create_task(sensor_loop(sensor, client, cfg))
        except* aiomqtt.MqttError as group:
            CONNECTED.set(0)
            log.warning("broker connection lost (%s); reconnecting in %.0fs", group.exceptions[0], backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


if __name__ == "__main__":
    asyncio.run(main())
