-- Stage 3: leader epochs for fencing. Runs automatically on a fresh volume; on an existing database:
--   docker compose exec -T postgres psql -U trustmesh -d trustmesh < infra/postgres/init/03-distribution.sql

-- The newest leader epoch per region. A new leader publishes its epoch before its first write;
-- every write first reads (and row-locks) this value and is rejected if its own epoch is older.
CREATE TABLE IF NOT EXISTS region_epochs (
    region      TEXT PRIMARY KEY,
    epoch       BIGINT NOT NULL,
    leader      TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE sensor_trust ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;
