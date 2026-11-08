-- Runs once, the first time the database volume is created.
-- Stage 1 adds the TrustMesh tables (sensors, readings, estimates, trust_history, alerts) here.
-- To re-run after editing: docker compose down -v && docker compose up -d
SELECT 'TrustMesh database initialised' AS status;
