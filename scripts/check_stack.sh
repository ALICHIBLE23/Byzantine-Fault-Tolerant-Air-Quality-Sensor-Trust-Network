#!/usr/bin/env bash
# Verifies every Step 0 service is up and answering. Run from the repo root.
set -u
FAILED=0
ok()   { echo "  [OK]   $1"; }
bad()  { echo "  [FAIL] $1"; FAILED=1; }

PGU="${POSTGRES_USER:-trustmesh}"
GP="${GRAFANA_PORT:-3000}"
PP="${PROMETHEUS_PORT:-9090}"

echo "Checking TrustMesh Step 0 stack..."

docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG \
  && ok "Redis responds to PING" || bad "Redis"

docker compose exec -T postgres pg_isready -U "$PGU" >/dev/null 2>&1 \
  && ok "PostgreSQL accepts connections" || bad "PostgreSQL"

TMP=$(mktemp)
docker compose exec -T mosquitto mosquitto_sub -t tm/check -C 1 -W 5 > "$TMP" 2>/dev/null &
SUB=$!
sleep 1
docker compose exec -T mosquitto mosquitto_pub -t tm/check -m hello >/dev/null 2>&1
wait $SUB
grep -q hello "$TMP" && ok "Mosquitto publish/subscribe round trip" || bad "Mosquitto"
rm -f "$TMP"

curl -sf "http://localhost:${PP}/-/ready" >/dev/null \
  && ok "Prometheus ready (http://localhost:${PP})" || bad "Prometheus"

curl -sf "http://localhost:${GP}/api/health" | grep -q '"ok"' \
  && ok "Grafana healthy (http://localhost:${GP})" || bad "Grafana"

echo
if [ "$FAILED" -eq 0 ]; then echo "All services OK. Step 0 complete."; else echo "Some checks failed: run 'docker compose logs <service>'"; exit 1; fi
