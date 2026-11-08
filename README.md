# TrustMesh

A Byzantine-resilient crowdsourced sensor network (Distributed Systems final project).

## Quick start (Step 0)

```bash
cp .env.example .env          # optional: defaults work as-is
docker compose up -d          # start infrastructure
./scripts/check_stack.sh      # verify everything answers
```

| Service    | Address                 | Notes                         |
|------------|-------------------------|-------------------------------|
| Mosquitto  | localhost:1883          | MQTT broker                   |
| Redis      | localhost:6379          | Reputation store, leader lease|
| PostgreSQL | localhost:5432          | user/pass/db: trustmesh       |
| Prometheus | http://localhost:9090   | Status > Targets              |
| Grafana    | http://localhost:3000   | admin / admin                 |

Python dev environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Useful commands:

```bash
docker compose ps                 # what is running
docker compose logs -f mosquitto  # follow a service's logs
docker compose down               # stop (keeps data)
docker compose down -v            # stop and wipe all data
```

## Layout

```
common/        shared message schemas, config, logging and metrics helpers
services/      sensors, aggregator, logging service, dashboard (later stages)
experiments/   experiment scripts and analysis
tests/         unit and integration tests
infra/         config for Mosquitto, PostgreSQL, Prometheus, Grafana
scripts/       helper scripts
```
