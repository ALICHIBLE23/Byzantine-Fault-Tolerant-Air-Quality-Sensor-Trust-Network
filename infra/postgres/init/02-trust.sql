-- Stage 2: sensor trust. Runs automatically on a fresh volume; on an existing database apply it with:
--   docker compose exec -T postgres psql -U trustmesh -d trustmesh < infra/postgres/init/02-trust.sql

-- Current trust state per sensor (one row each, overwritten every window).
CREATE TABLE IF NOT EXISTS sensor_trust (
    sensor_id     TEXT PRIMARY KEY,
    region        TEXT NOT NULL,
    reputation    DOUBLE PRECISION NOT NULL,
    spatial       DOUBLE PRECISION NOT NULL,   -- signal 1, this window
    temporal      DOUBLE PRECISION NOT NULL,   -- signal 2, this window
    value         DOUBLE PRECISION,            -- what the sensor reported (window average)
    reference     DOUBLE PRECISION,            -- what its trusted neighbours reported
    flagged       BOOLEAN NOT NULL,
    reason        TEXT NOT NULL DEFAULT '',
    window_start  TIMESTAMPTZ NOT NULL,
    replica_id    TEXT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sensor_trust_flagged ON sensor_trust (flagged, region);

-- Reputation of every sensor in every window: used to measure detection time and false flags.
CREATE TABLE IF NOT EXISTS trust_history (
    window_start  TIMESTAMPTZ NOT NULL,
    sensor_id     TEXT NOT NULL,
    region        TEXT NOT NULL,
    reputation    DOUBLE PRECISION NOT NULL,
    flagged       BOOLEAN NOT NULL,
    PRIMARY KEY (sensor_id, window_start)
);
CREATE INDEX IF NOT EXISTS trust_history_window ON trust_history (window_start);
