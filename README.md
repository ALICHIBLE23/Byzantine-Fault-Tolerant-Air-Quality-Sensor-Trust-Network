# TrustMesh

A Byzantine-resilient crowdsourced sensor network (Distributed Systems final project).

## Run it

```bash
cp .env.example .env              # optional: defaults work as-is
docker compose up -d --build      # build the Python image and start everything
./scripts/check_stack.sh          # infrastructure health
python -m experiments.check_stage1   # end-to-end check (wait ~30 s after start)
python -m experiments.check_stage2   # trust check (set BEHAVIOR_MIX, wait 3-4 min)
python -m experiments.check_stage3   # replication and leadership check
python -m experiments.check_stage4   # border exchange and shared reputations
python -m experiments.check_stage5   # alerting accuracy vs ground truth (run after 10+ min)
```

## Quorum alerting

Every aggregator replica, leader and standby, votes once per window on whether its region is above
`ALERT_THRESHOLD` (35.5 ug/m3). The alerting service raises an alert only when `ALERT_QUORUM` of the
2 replicas agree, so a single faulty replica can neither trigger nor hide an alert. Alerts close
after `ALERT_CLEAR_WINDOWS` consecutive clear windows.

Faulty replica experiment (in `.env`, then `docker compose up -d aggregator-b2 alerting`):

```
B2_VOTE_BIAS=30     # aggregator-b2 inflates every vote by 30 ug/m3
ALERT_QUORUM=2      # its lies are blocked;  ALERT_QUORUM=1: they become false alarms
```

## Experiments

| Script | Experiment | Needs the stack |
|--------|------------|-----------------|
| `experiments/trust_lab.py --sweep` | E1 accuracy vs % faulty sensors | no |
| `experiments/failover.py` | E7 crash / stop / pause a leader | yes |
| `experiments/concurrency_bench.py` | E8 concurrency strategies vs contention | yes (Redis) |
| `experiments/scalability.py` | E5 throughput, latency, CPU vs fleet size | yes (~10 min) |
| `experiments/tradeoffs.py` | E10 threshold, reputation speed, window size | no |
| `experiments/check_stage5.py` | alert precision / recall vs baselines | yes |

All results (CSV and PNG) are written to `results/`.

## Border exchange and concurrency control

Readings are published to `readings/<region>/<cell>/<sensor>` (1 km cells). Each replica also
subscribes to the strip of cells along its border, so sensors at a region edge are judged against
their full neighbourhood. A border sensor is evaluated by every region it borders; its reputation is
one shared Redis record (`rep:<sensor>`) that every evaluating leader updates after each window, each
as a partial vote of 1/k (k = number of regions judging it).

Concurrent updates of a shared record are a read-modify-write race. `CONCURRENCY` selects the control:

| Strategy   | How                                                   | Cost                         |
|------------|-------------------------------------------------------|------------------------------|
| none       | read, compute, write                                  | loses updates under contention |
| optimistic | WATCH / MULTI / EXEC, retry if the record changed      | retries under contention     |
| lock       | distributed lock (SET NX PX + token), then update     | writers wait for each other  |
| atomic     | the whole update as one Lua script inside Redis       | none, but logic lives in Redis |

```bash
python -m experiments.concurrency_bench                 # E8: all four strategies vs contention
python -m experiments.trust_lab --mix "offset_high:0.2" --no-border   # compare without border exchange
```

## Replication and failover

Each region has a leader lease in Redis (`lease:<region>`, 3 s TTL, renewed every second).
The leader writes; the other replica of the partition computes the same windows as a hot
standby. Every lease change increments `epoch:<region>`, a fencing token that the database
checks on every write.

```bash
python -m experiments.failover --mode kill   # crash a leader: takeover after the TTL
python -m experiments.failover --mode stop   # graceful stop: lease released, instant takeover
python -m experiments.failover --mode pause  # freeze a leader, wake it up after a takeover
docker compose exec redis redis-cli MGET lease:r-0-0 lease:r-0-1 lease:r-1-0 lease:r-1-1
```

To see database fencing reject a stale leader, set `FENCING_SELF_CHECK=false` in `.env`,
run `docker compose up -d aggregator-b1 aggregator-b2`, then the pause experiment.

## Faulty sensors and the trust lab

Set `BEHAVIOR_MIX` in `.env` to make part of the fleet faulty or malicious, then
`docker compose up -d sensors evaluator`:

```
BEHAVIOR_MIX=offset_high:0.1,hider:0.05,stuck:0.05
```

| Behavior      | What it does                                            |
|---------------|---------------------------------------------------------|
| offset_high   | Malicious: inflates readings to trigger false alarms    |
| hider         | Malicious: under-reports to hide real pollution         |
| on_off        | Malicious: honest to build trust, then lies in bursts   |
| stuck         | Fault: freezes on one value                             |
| drift         | Fault: slowly growing bias                              |
| spike         | Fault: frequent random spikes                           |

The trust algorithm can be evaluated offline in seconds, without Docker:

```bash
python -m experiments.trust_lab --mix "offset_high:0.2"
python -m experiments.trust_lab --sweep --attack offset_high   # plot in results/
```

| What               | Where                          | Notes                                  |
|--------------------|--------------------------------|----------------------------------------|
| Grafana            | http://localhost:3000          | admin / admin, opens on TrustMesh Overview |
| Prometheus         | http://localhost:9090          | Status > Targets                       |
| Logging service    | http://localhost:8081/logs     | `?service=aggregator&level=WARNING`    |
| Logging API docs   | http://localhost:8081/docs     | auto-generated by FastAPI              |
| MQTT broker        | localhost:1883                 | watch `readings/#` in MQTT Explorer    |
| PostgreSQL         | localhost:5432                 | trustmesh / trustmesh / trustmesh      |

## Components (Stage 5)

| Service      | Code                         | Role |
|--------------|------------------------------|------|
| sensors      | services/sensors/            | Simulates the sensor fleet; publishes readings over MQTT |
| aggregator-a1, a2 | services/aggregator/    | Partition A (r-0-0, r-0-1): two replicas, one leader per region |
| aggregator-b1, b2 | services/aggregator/    | Partition B (r-1-0, r-1-1): two replicas, one leader per region |
| logging-svc  | services/logging_svc/        | Central log collection (stdout + PostgreSQL + HTTP API) |
| mosquitto    | infra/mosquitto/             | MQTT message broker |
| postgres     | infra/postgres/init/         | Sensors, readings, estimates, logs |
| alerting     | services/alerting/           | Quorum of replica votes -> pollution alerts (PostgreSQL) |
| evaluator    | services/evaluator/          | Evaluation only: publishes ground truth to Prometheus |
| redis        |                              | Leader leases, fencing epochs, shared trust state |
| prometheus   | infra/prometheus/            | Scrapes /metrics from every service |
| grafana      | infra/grafana/               | Provisioned data sources and dashboard |

## Development

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest                                   # unit tests, no running stack needed
docker compose up -d --build aggregator  # rebuild and restart one service after code changes
docker compose logs -f logging-svc       # every service's logs in one stream
docker compose down -v                   # stop and wipe all data (needed after schema changes)
```
