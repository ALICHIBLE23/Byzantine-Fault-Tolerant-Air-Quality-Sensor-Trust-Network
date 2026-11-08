"""Central logging service: every component POSTs its logs here.

Logs are printed to stdout (so `docker compose logs -f logging-svc` shows the whole system in one
stream) and stored in PostgreSQL in batches (so Grafana and the API can query them).
"""
from __future__ import annotations

import asyncio
import contextlib
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psycopg
from fastapi import FastAPI, Query, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from pydantic import BaseModel, Field

from common.config import load_config
from common.logclient import setup_logging

cfg = load_config("logging-svc")
log = setup_logging(cfg, ship=False)  # never ship our own logs to ourselves

MAX_BUFFER = 100_000
buffer: list[tuple] = []

RECEIVED = Counter("tm_logs_received_total", "Log records received", ["service", "level"])
STORED = Counter("tm_logs_stored_total", "Log records written to the database")
DROPPED = Counter("tm_logs_dropped_total", "Log records dropped because the buffer overflowed")
BUFFERED = Gauge("tm_logs_buffered", "Log records waiting to be written")
DB_CONNECTED = Gauge("tm_logs_db_connected", "1 when connected to the database")

INSERT_LOG = """
INSERT INTO logs (ts, service, instance, level, logger, message)
VALUES (%s, %s, %s, %s, %s, %s)
"""


class LogRecordIn(BaseModel):
    ts: float
    service: str = Field(max_length=64)
    instance: str = Field(default="", max_length=128)
    level: str = Field(default="INFO", max_length=16)
    logger: str = Field(default="", max_length=128)
    message: str = Field(max_length=8000)


async def writer() -> None:
    conn: psycopg.AsyncConnection | None = None
    backoff = 1.0
    while True:
        await asyncio.sleep(1.0)
        BUFFERED.set(len(buffer))
        if not buffer:
            continue
        batch = buffer[:]
        buffer.clear()
        try:
            if conn is None or conn.closed:
                conn = await psycopg.AsyncConnection.connect(cfg.pg_dsn, autocommit=True)
                DB_CONNECTED.set(1)
                log.info("connected to database")
                backoff = 1.0
            async with conn.cursor() as cur:
                await cur.executemany(INSERT_LOG, batch)
            STORED.inc(len(batch))
        except psycopg.Error as exc:
            buffer[0:0] = batch  # put them back in front, keep order
            DB_CONNECTED.set(0)
            log.warning("log write failed (%s); retrying in %.0fs", exc, backoff)
            if conn is not None:
                await conn.close()
            conn = None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(writer())
    log.info("logging service started")
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


app = FastAPI(title="TrustMesh logging service", lifespan=lifespan)


@app.post("/logs", status_code=202)
async def ingest(records: list[LogRecordIn]) -> dict:
    lines = []
    for r in records:
        ts = datetime.fromtimestamp(r.ts, tz=timezone.utc)
        buffer.append((ts, r.service, r.instance, r.level.upper(), r.logger, r.message))
        RECEIVED.labels(r.service, r.level.upper()).inc()
        lines.append(f"{ts:%H:%M:%S.%f}"[:-3] + f" {r.level:<7} {r.service}/{r.instance}: {r.message}\n")
    sys.stdout.write("".join(lines))
    sys.stdout.flush()
    if len(buffer) > MAX_BUFFER:
        overflow = len(buffer) - MAX_BUFFER
        del buffer[:overflow]
        DROPPED.inc(overflow)
    return {"accepted": len(records)}


@app.get("/logs")
async def recent_logs(service: str | None = None, level: str | None = None,
                      limit: int = Query(100, ge=1, le=1000)) -> list[dict]:
    where, params = [], []
    if service:
        where.append("service = %s")
        params.append(service)
    if level:
        where.append("level = %s")
        params.append(level.upper())
    sql = "SELECT ts, service, instance, level, message FROM logs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ts DESC LIMIT %s"
    params.append(limit)
    async with await psycopg.AsyncConnection.connect(cfg.pg_dsn) as conn:
        cur = await conn.execute(sql, params)
        rows = await cur.fetchall()
    return [{"ts": ts.isoformat(), "service": s, "instance": i, "level": lv, "message": m}
            for ts, s, i, lv, m in rows]


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "buffered": len(buffer)}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
