-- TrustMesh schema (Stage 1). Runs automatically on a fresh database volume.
-- After editing: docker compose down -v && docker compose up -d   (wipes all data)

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id      TEXT PRIMARY KEY,
    region         TEXT NOT NULL,
    x_km           DOUBLE PRECISION NOT NULL,
    y_km           DOUBLE PRECISION NOT NULL,
    lat            DOUBLE PRECISION NOT NULL,
    lon            DOUBLE PRECISION NOT NULL,
    behavior       TEXT NOT NULL DEFAULT 'honest',  -- ground-truth label, never read by aggregators
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Raw readings. The primary key makes inserts idempotent: a re-delivered message is ignored.
CREATE TABLE IF NOT EXISTS readings (
    sensor_id    TEXT NOT NULL,
    seq          BIGINT NOT NULL,
    region       TEXT NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,             -- sensor's clock
    value        DOUBLE PRECISION NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),  -- aggregator's clock
    PRIMARY KEY (sensor_id, seq)
);
CREATE INDEX IF NOT EXISTS readings_region_ts ON readings (region, ts);

-- One row per region, window and algorithm, so all methods are compared on identical data.
-- epoch is the leader-lease fencing token (Stage 3): stale leaders cannot overwrite newer rows.
CREATE TABLE IF NOT EXISTS estimates (
    region        TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_ms     INTEGER NOT NULL,
    algorithm     TEXT NOT NULL,
    estimate      DOUBLE PRECISION NOT NULL,
    n_readings    INTEGER NOT NULL,
    n_sensors     INTEGER NOT NULL,
    replica_id    TEXT NOT NULL,
    epoch         BIGINT NOT NULL DEFAULT 0,
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (region, window_start, algorithm)
);
CREATE INDEX IF NOT EXISTS estimates_window ON estimates (window_start);

CREATE TABLE IF NOT EXISTS logs (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL,
    service   TEXT NOT NULL,
    instance  TEXT NOT NULL DEFAULT '',
    level     TEXT NOT NULL,
    logger    TEXT NOT NULL DEFAULT '',
    message   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS logs_ts ON logs (ts DESC);
CREATE INDEX IF NOT EXISTS logs_service_ts ON logs (service, ts DESC);
