"""Generates the provisioned Grafana dashboard. Edit here, then run:
    python scripts/build_dashboard.py && docker compose restart grafana
"""
import json
from pathlib import Path

PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "grafana-postgresql-datasource", "uid": "postgres"}
OUT = Path(__file__).resolve().parent.parent / "infra/grafana/provisioning/dashboards/json/trustmesh-overview.json"
_next = [0]


def nid():
    _next[0] += 1
    return _next[0]


def stat(title, expr, x, y, w=4, unit="none", decimals=None, steps=None, desc=""):
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": steps or [{"color": "green", "value": None}]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {"id": nid(), "type": "stat", "title": title, "description": desc, "datasource": PROM,
            "gridPos": {"h": 4, "w": w, "x": x, "y": y},
            "targets": [{"refId": "A", "datasource": PROM, "expr": expr}],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "value", "graphMode": "area", "textMode": "auto", "justifyMode": "auto",
                        "orientation": "auto"}}


def timeseries(title, targets, x, y, w=12, h=8, unit="none", desc="", overrides=None):
    return {"id": nid(), "type": "timeseries", "title": title, "description": desc, "datasource": PROM,
            "gridPos": {"h": h, "w": w, "x": x, "y": y},
            "targets": [{"refId": chr(65 + i), "datasource": PROM, "expr": e, "legendFormat": l}
                        for i, (e, l) in enumerate(targets)],
            "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 0,
                                                                   "showPoints": "never"}},
                            "overrides": overrides or []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "none"}}}


def sql_target(sql):
    return {"refId": "A", "datasource": PG, "format": "table", "rawQuery": True, "editorMode": "code", "rawSql": sql}


def color_override(name, color, width=None, dash=False):
    props = [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]
    if width:
        props.append({"id": "custom.lineWidth", "value": width})
    if dash:
        props.append({"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}})
    return {"matcher": {"id": "byName", "options": name}, "properties": props}


panels = [
    # ---- headline numbers
    stat("Active sensors", "sum(tm_sensor_active)", 0, 0),
    stat("Readings / s", "sum(max by (region) (rate(tm_agg_readings_total[30s])))", 4, 0, unit="reqps", decimals=0,
         desc="Both replicas of a region receive every reading; counted once per region."),
    stat("Flagged sensors", "sum(tm_trust_flagged)", 8, 0,
         steps=[{"color": "green", "value": None}, {"color": "orange", "value": 1}],
         desc="Sensors whose reputation fell below the flag threshold."),
    stat("Configured faulty (truth)", "sum(tm_truth_faulty_sensors)", 12, 0,
         desc="Evaluation only: how many sensors were configured faulty via BEHAVIOR_MIX."),
    stat("TrustMesh error now", 'avg(abs(max by (region) (tm_agg_estimate{algorithm="trustmesh"}) - on(region) max by (region) (tm_truth)))',
         16, 0, decimals=2, steps=[{"color": "green", "value": None}, {"color": "orange", "value": 2},
                                   {"color": "red", "value": 5}],
         desc="Average absolute error vs ground truth across regions, ug/m3 (evaluation only)."),
    stat("Window latency p95", "histogram_quantile(0.95, sum by (le) (rate(tm_agg_window_latency_seconds_bucket[1m])))",
         20, 0, unit="s", decimals=2, steps=[{"color": "green", "value": None}, {"color": "orange", "value": 4.5},
                                            {"color": "red", "value": 6}],
         desc="Window end to estimate stored; includes the 2 s grace period for late readings."),

    # ---- trust view
    {"id": nid(), "type": "geomap", "title": "Sensor trust map", "datasource": PG,
     "description": "Each dot is a sensor, coloured by reputation: green trusted, red flagged.",
     "gridPos": {"h": 13, "w": 12, "x": 0, "y": 4},
     "targets": [sql_target(
         "SELECT s.lat, s.lon, s.sensor_id, s.region, COALESCE(t.reputation, 0.5) AS trust, "
         "COALESCE(t.reason, '') AS reason FROM sensors s LEFT JOIN sensor_trust t USING (sensor_id)")],
     "fieldConfig": {"defaults": {"color": {"mode": "continuous-RdYlGr"}, "min": 0, "max": 1, "decimals": 2},
                     "overrides": []},
     "options": {"view": {"id": "coords", "lat": 33.895, "lon": 35.534, "zoom": 12.2, "allLayers": True},
                 "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True},
                 "basemap": {"type": "osm-standard", "name": "OpenStreetMap"},
                 "layers": [{"type": "markers", "name": "Sensors", "tooltip": True,
                             "location": {"mode": "coords", "latitude": "lat", "longitude": "lon"},
                             "config": {"showLegend": True,
                                        "style": {"size": {"fixed": 6}, "opacity": 0.9,
                                                  "color": {"field": "trust", "fixed": "green"},
                                                  "symbol": {"mode": "fixed", "fixed": "img/icons/marker/circle.svg"}}}}],
                 "tooltip": {"mode": "details"}}},
    {"id": nid(), "type": "table", "title": "Flagged sensors", "datasource": PG,
     "description": "What each flagged sensor reported versus what its trusted neighbours reported.",
     "gridPos": {"h": 13, "w": 12, "x": 12, "y": 4},
     "targets": [sql_target(
         "SELECT t.sensor_id AS sensor, t.region, round(t.reputation::numeric, 2) AS reputation, t.reason, "
         "round(t.value::numeric, 1) AS reported, round(t.reference::numeric, 1) AS neighbours, "
         "s.behavior AS \"actual (truth)\" FROM sensor_trust t JOIN sensors s USING (sensor_id) "
         "WHERE t.flagged ORDER BY t.reputation")],
     "fieldConfig": {"defaults": {}, "overrides": [
         {"matcher": {"id": "byName", "options": "reputation"},
          "properties": [{"id": "custom.cellOptions", "value": {"type": "color-background"}},
                         {"id": "color", "value": {"mode": "continuous-RdYlGr"}},
                         {"id": "min", "value": 0}, {"id": "max", "value": 1},
                         {"id": "decimals", "value": 2}]}]},
     "options": {"showHeader": True, "cellHeight": "sm"}},

    # ---- estimates vs truth
    timeseries("Estimates vs ground truth: $region",
               [('max(tm_truth{region="$region"})', "ground truth"),
                ('max(tm_agg_estimate{region="$region", algorithm="trustmesh"})', "trustmesh"),
                ('max(tm_agg_estimate{region="$region", algorithm="mean"})', "mean"),
                ('max(tm_agg_estimate{region="$region", algorithm="median"})', "median")],
               0, 17, desc="ug/m3. Pick the region at the top of the dashboard.",
               overrides=[color_override("ground truth", "white", 3, dash=True),
                          color_override("trustmesh", "green", 3),
                          color_override("mean", "red"), color_override("median", "orange")]),
    timeseries("Error vs ground truth by algorithm (all regions)",
               [('avg by (algorithm) (abs(max by (region, algorithm) (tm_agg_estimate) - on(region) group_left max by (region) (tm_truth)))', "{{algorithm}}")],
               12, 17, desc="Mean absolute error in ug/m3 (evaluation only).",
               overrides=[color_override("trustmesh", "green", 3), color_override("mean", "red"),
                          color_override("median", "orange"), color_override("trimmed_mean", "yellow")]),
    timeseries("Flagged sensors per region", [("max by (region) (tm_trust_flagged)", "{{region}}")], 0, 25),
    timeseries("Flag events by reason", [("sum by (reason) (increase(tm_trust_flag_events_total[1m]))", "{{reason}}")],
               12, 25, desc="Sensors newly flagged per minute, by the signal that caught them."),

    # ---- pipeline health
    timeseries("Readings ingested per region", [("max by (region) (rate(tm_agg_readings_total[30s]))", "{{region}}")],
               0, 33, unit="reqps"),
    timeseries("Window latency", [
        ("histogram_quantile(0.50, sum by (le) (rate(tm_agg_window_latency_seconds_bucket[1m])))", "p50"),
        ("histogram_quantile(0.95, sum by (le) (rate(tm_agg_window_latency_seconds_bucket[1m])))", "p95"),
        ("histogram_quantile(0.95, sum by (le) (rate(tm_trust_compute_seconds_bucket[1m])))", "trust compute p95"),
    ], 12, 33, unit="s"),
    timeseries("Anomalies in the pipeline", [
        ("sum(rate(tm_agg_duplicates_total[1m]))", "duplicates/s"),
        ("sum(rate(tm_agg_late_total[1m]))", "late/s"),
        ("sum(rate(tm_agg_invalid_total[1m]))", "invalid/s"),
        ("sum(rate(tm_sensor_publish_errors_total[1m]))", "publish errors/s"),
        ("sum(rate(tm_agg_db_dropped_total[1m]))", "DB rows dropped/s"),
        ("sum(rate(tm_agg_fenced_writes_total[1m]))", "fenced writes/s"),
    ], 0, 41, unit="reqps"),
    {"id": nid(), "type": "table", "title": "Recent logs (all services)", "datasource": PG,
     "gridPos": {"h": 8, "w": 12, "x": 12, "y": 41},
     "targets": [sql_target("SELECT ts AS \"time\", service, level, message FROM logs "
                            "WHERE $__timeFilter(ts) ORDER BY ts DESC LIMIT 200")],
     "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True, "cellHeight": "sm"}},
]

# ---- distribution view, inserted below the trust view (everything after it moves down)
DIST_Y, DIST_H = 17, 8
for panel in panels:
    if panel["gridPos"]["y"] >= DIST_Y:
        panel["gridPos"]["y"] += DIST_H
panels += [
    {"id": nid(), "type": "state-timeline", "title": "Leader lease per region",
     "description": "Which replica holds each region's leader lease. Kill or pause a leader and watch it move.",
     "datasource": PROM, "gridPos": {"h": DIST_H, "w": 12, "x": 0, "y": DIST_Y},
     "targets": [{"refId": "A", "datasource": PROM,
                  "expr": 'max by (region, job_instance) (label_replace(tm_agg_is_leader, "job_instance", "$1", "instance", "(.*):.*"))',
                  "legendFormat": "{{region}} {{job_instance}}"}],
     "fieldConfig": {"defaults": {
         # No threshold colouring: with it, the state timeline shows threshold buckets ("< 1", "1+")
         # instead of the mapped names. The mappings below set both text and colour.
         "color": {"mode": "fixed", "fixedColor": "#3a3a3a"},
         "mappings": [{"type": "range", "options": {"from": -0.5, "to": 0.5,
                                                    "result": {"text": "standby", "color": "#3a3a3a", "index": 0}}},
                      {"type": "range", "options": {"from": 0.5, "to": 1.5,
                                                    "result": {"text": "LEADER", "color": "green", "index": 1}}}],
         "custom": {"fillOpacity": 80, "lineWidth": 0}}, "overrides": []},
     "options": {"showValue": "auto", "mergeValues": True, "rowHeight": 0.85, "alignValue": "center",
                 "legend": {"showLegend": False}, "tooltip": {"mode": "single"}}},
    timeseries("Lease epoch per region (fencing token)", [("max by (region) (tm_agg_epoch)", "{{region}}")],
               12, DIST_Y, h=DIST_H,
               desc="Increments every time a region gets a new leader. Writes from older epochs are rejected."),
]

# ---- border exchange and concurrency control (Stage 4), at the bottom
BOTTOM = max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in panels)
panels += [
    timeseries("Border readings received per replica",
               [('sum by (job_instance) (label_replace(rate(tm_agg_border_readings_total[1m]), "job_instance", "$1", '
                 '"instance", "(.*):.*"))', "{{job_instance}}")],
               0, BOTTOM, w=8, unit="reqps",
               desc="Readings from neighbouring regions' border cells, used as spatial context."),
    timeseries("Shared reputation updates", [
        ("sum by (result) (rate(tm_rep_updates_total[1m]))", "{{result}}/s"),
        ("sum(rate(tm_rep_conflicts_total[1m]))", "optimistic conflicts/s"),
    ], 8, BOTTOM, w=8, unit="reqps",
        desc="Evidence applied to shared border reputations. Each optimistic conflict is a concurrent write "
             "that would have been a lost update without concurrency control."),
    timeseries("Shared update latency", [
        ("histogram_quantile(0.95, sum by (le, strategy) (rate(tm_rep_update_seconds_bucket[1m])))", "p95 {{strategy}}"),
        ("histogram_quantile(0.95, sum by (le) (rate(tm_rep_lock_wait_seconds_bucket[1m])))", "lock wait p95"),
    ], 16, BOTTOM, w=8, unit="s"),
]

# ---- quorum alerting (Stage 5)
BOTTOM = max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in panels)
panels += [
    {"id": nid(), "type": "state-timeline", "title": "Pollution alerts per region",
     "description": "An alert is open while a quorum of replicas agrees the region is above the threshold.",
     "datasource": PROM, "gridPos": {"h": 7, "w": 12, "x": 0, "y": BOTTOM},
     "targets": [{"refId": "A", "datasource": PROM, "expr": "max by (region) (tm_alerts_open)",
                  "legendFormat": "{{region}}"}],
     "fieldConfig": {"defaults": {
         "color": {"mode": "fixed", "fixedColor": "#3a3a3a"},
         "mappings": [{"type": "range", "options": {"from": -0.5, "to": 0.5,
                                                    "result": {"text": "ok", "color": "#3a3a3a", "index": 0}}},
                      {"type": "range", "options": {"from": 0.5, "to": 1.5,
                                                    "result": {"text": "ALERT", "color": "red", "index": 1}}}],
         "custom": {"fillOpacity": 85, "lineWidth": 0}}, "overrides": []},
     "options": {"showValue": "auto", "mergeValues": True, "rowHeight": 0.85, "alignValue": "center",
                 "legend": {"showLegend": False}, "tooltip": {"mode": "single"}}},
    timeseries("Alert decisions per minute", [
        ("sum by (decision) (increase(tm_alert_decisions_total[1m]))", "{{decision}}"),
    ], 12, BOTTOM, h=7,
        desc="blocked = some replicas voted over the threshold but fewer than the quorum.",
        overrides=[color_override("alarm", "red"), color_override("blocked", "orange"),
                   color_override("clear", "green")]),
    {"id": nid(), "type": "table", "title": "Alerts", "datasource": PG,
     "gridPos": {"h": 8, "w": 24, "x": 0, "y": BOTTOM + 7},
     "targets": [sql_target(
         "SELECT region, status, opened_window AS opened, closed_window AS closed, "
         "round(peak_estimate::numeric, 1) AS \"peak ug/m3\", votes_over || '/' || votes_total AS votes, quorum "
         "FROM alerts ORDER BY opened_window DESC LIMIT 50")],
     "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True, "cellHeight": "sm"}},
]

dashboard = {
    "uid": "trustmesh-overview", "title": "TrustMesh Overview", "tags": ["trustmesh"], "timezone": "browser",
    "schemaVersion": 39, "version": 9, "editable": True, "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"}, "panels": panels, "annotations": {"list": []},
    "templating": {"list": [{
        "name": "region", "label": "Region", "type": "query", "datasource": PROM,
        "query": {"query": "label_values(tm_agg_estimate, region)", "refId": "region"},
        "definition": "label_values(tm_agg_estimate, region)", "refresh": 2, "sort": 1,
        "includeAll": False, "multi": False, "current": {"text": "r-1-1", "value": "r-1-1"}}]},
}

if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, indent=2))
    print(f"wrote {OUT} ({len(panels)} panels)")
