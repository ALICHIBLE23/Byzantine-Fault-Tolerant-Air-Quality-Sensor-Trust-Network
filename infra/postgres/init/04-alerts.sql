-- Stage 5: quorum alerts. Runs automatically on a fresh volume; on an existing database:
--   docker compose exec -T postgres psql -U trustmesh -d trustmesh < infra/postgres/init/04-alerts.sql

-- One row per region per window: what the replicas voted and what was decided.
CREATE TABLE IF NOT EXISTS alert_decisions (
    region        TEXT NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    decision      TEXT NOT NULL,          -- alarm | blocked | clear
    votes_over    INTEGER NOT NULL,
    votes_total   INTEGER NOT NULL,
    estimate      DOUBLE PRECISION NOT NULL,
    voters        TEXT NOT NULL DEFAULT '',  -- replicas that voted "over"
    decided_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (region, window_start)
);

-- Alert events: open on the first alarm window, closed after consecutive clear windows.
CREATE TABLE IF NOT EXISTS alerts (
    alert_id       TEXT PRIMARY KEY,        -- region@window: deterministic, so writes are idempotent
    region         TEXT NOT NULL,
    status         TEXT NOT NULL,           -- open | closed
    opened_window  TIMESTAMPTZ NOT NULL,
    closed_window  TIMESTAMPTZ,
    peak_estimate  DOUBLE PRECISION NOT NULL,
    votes_over     INTEGER NOT NULL,
    votes_total    INTEGER NOT NULL,
    quorum         INTEGER NOT NULL,
    opened_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS alerts_region ON alerts (region, opened_window DESC);
