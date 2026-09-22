from datetime import date
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse

from common.db import fetch_all
from common.logging_conf import log
from common.settings import NO_DATA_SECONDS, VEHICLE_COUNT
from common.settings import DATA_DIR
from common.publication import ALGORITHM_VERSION, export_matches

app = FastAPI(title="Fleet Lambda Platform", version="0.1.0")


@app.get('/', response_class=HTMLResponse)
def dashboard():
    return Path(__file__).with_name('dashboard.html').read_text(encoding='utf-8')


@app.middleware("http")
async def log_request(request, call_next):
    response = await call_next(request)
    log("api", "request", path=request.url.path, status=response.status_code)
    return response


@app.get("/health")
def health():
    return {"status": "alive"}


def pipeline_status():
    summary = fetch_all("""SELECT
        (SELECT GREATEST(EXTRACT(EPOCH FROM (now()-max(accepted_at))),
            (SELECT EXTRACT(EPOCH FROM (now()-started_at)) FROM simulation_clock WHERE id=1)
            - EXTRACT(EPOCH FROM (max(event_ts)-timestamptz '2026-03-01 00:00:00+00'))
              * (SELECT day_seconds FROM simulation_clock WHERE id=1) / 86400.0)
         FROM stream_event_keys) AS last_event_age_seconds,
        COALESCE(sum(rows_in), 0) AS rows_in,
        COALESCE(sum(rows_rejected), 0) AS rows_rejected
        FROM pipeline_batches""")[0]
    age = summary["last_event_age_seconds"]
    healthy = age is not None and age <= NO_DATA_SECONDS
    return {"status": "healthy" if healthy else "no_recent_data", "healthy": healthy,
            "threshold_real_seconds": NO_DATA_SECONDS, **summary}


@app.get("/health/pipeline")
def health_pipeline():
    try:
        status = pipeline_status()
        # Decimal from PostgreSQL is converted explicitly for this Response.
        status = {key: float(value) if isinstance(value, Decimal) else value for key, value in status.items()}
        return JSONResponse(status, status_code=200 if status["healthy"] else 503)
    except Exception:
        log("api", "database_unavailable")
        return JSONResponse({"status": "database_unavailable", "healthy": False}, status_code=503)


@app.get("/health/reports")
def health_reports():
    """Batch health is separate from live ingestion and financial completeness."""
    try:
        summary = fetch_all("""WITH latest AS (
            SELECT DISTINCT ON (dt) dt,status,started_at FROM pipeline_runs ORDER BY dt,started_at DESC
        ) SELECT count(*) FILTER (WHERE status='failed') AS failed_dates,
            count(*) FILTER (WHERE status='running' AND started_at < now()-interval '20 minutes') AS stalled_dates
            FROM latest""")[0]
        publication = fetch_all("""SELECT max(dt) FILTER (WHERE export_status='published') AS latest_report,
            count(*) FILTER (WHERE export_status <> 'published') AS pending_exports
            FROM daily_report_status""")[0]
        expected = fetch_all("SELECT max(max_event_ts)::date - 1 AS expected_report FROM pipeline_batches")[0]["expected_report"]
        exports = fetch_all("SELECT dt,output_sha256,algorithm_version FROM daily_report_status WHERE export_status='published'")
        bad_exports = sum(not export_matches(DATA_DIR / 'reports' / ('profitability_' + str(r['dt']) + '.json'), r['output_sha256']) for r in exports)
        old_versions = sum(r['algorithm_version'] != ALGORITHM_VERSION for r in exports)
        missing = fetch_all("""SELECT count(*) AS n FROM generate_series(date '2026-03-01',
            %s::date,interval '1 day') AS d(dt) LEFT JOIN daily_report_status s ON s.dt=d.dt
            WHERE s.dt IS NULL""", (expected,))[0]['n']
        healthy = (not summary["failed_dates"] and not summary["stalled_dates"] and
                   not publication["pending_exports"] and not bad_exports and not old_versions and not missing and expected is not None and
                   publication["latest_report"] is not None and publication["latest_report"] >= expected)
        from fastapi.encoders import jsonable_encoder
        return JSONResponse(jsonable_encoder({"healthy": healthy, "status": "healthy" if healthy else "degraded",
                            "expected_report": expected, "missing_dates": missing,
                            "invalid_exports": bad_exports, "outdated_dates": old_versions,
                            "algorithm_version": ALGORITHM_VERSION,
                            **summary, **publication}), status_code=200 if healthy else 503)
    except Exception as exc:
        log("api", "report_health_unavailable", error=str(exc))
        return JSONResponse({"healthy": False, "status": "database_unavailable"}, status_code=503)


@app.get("/metrics/fleet")
def fleet():
    result = fetch_all("""WITH reference AS (SELECT max(event_ts) AS ts FROM rt_vehicle_state)
        SELECT count(*) AS reporting_vehicles,
               count(*) FILTER (WHERE status <> 'idle') AS active_vehicles,
               count(*) FILTER (WHERE status = 'idle')::float / NULLIF(count(*), 0) AS idle_ratio
        FROM rt_vehicle_state, reference WHERE event_ts >= reference.ts - interval '30 minutes'""")[0]
    trips = fetch_all("""SELECT count(*) AS trips_last_simulated_hour,
        COALESCE(sum(fare_cents),0) AS earnings_last_simulated_hour_cents
        FROM completed_trips WHERE completed_at >=
            (SELECT max(event_ts) FROM rt_vehicle_state) - interval '1 hour'""")[0]
    return {"currency": "LKR", "accuracy": "indicative", "registered_vehicles": VEHICLE_COUNT,
            "time_basis": "simulated UTC; anchored to latest processed event", **result, **trips}


@app.get("/metrics/zones")
def zones():
    return fetch_all("""WITH reference AS (SELECT max(event_ts) AS ts FROM rt_vehicle_state),
        live AS (
            SELECT zone, count(*) AS reporting_vehicles,
                   count(*) FILTER (WHERE status <> 'idle') AS active_vehicles,
                   count(*) FILTER (WHERE status = 'idle')::float / NULLIF(count(*),0) AS idle_ratio
            FROM rt_vehicle_state, reference WHERE event_ts >= reference.ts - interval '30 minutes'
            GROUP BY zone
        ), trips AS (
            SELECT zone, count(*) AS trips_last_hour, sum(fare_cents) AS earnings_cents
            FROM completed_trips, reference WHERE completed_at >= reference.ts - interval '1 hour'
            GROUP BY zone
        ) SELECT COALESCE(live.zone,trips.zone) AS zone, live.reporting_vehicles,
            live.active_vehicles, live.idle_ratio, COALESCE(trips.trips_last_hour,0) AS trips_last_hour,
            COALESCE(trips.earnings_cents,0) AS earnings_cents
        FROM live FULL OUTER JOIN trips ON live.zone=trips.zone ORDER BY zone""")


@app.get("/metrics/time-of-day")
def time_of_day(report_date: date):
    return fetch_all("""SELECT zone, time_of_day_bucket, count(*) AS trips, sum(fare_cents) AS earnings_cents
        FROM completed_trips WHERE completed_at >= %s::date
        AND completed_at < %s::date + interval '1 day'
        GROUP BY zone, time_of_day_bucket ORDER BY zone, time_of_day_bucket""", (report_date, report_date))


@app.get("/alerts/active")
def alerts(idle_minutes: int = Query(default=15, ge=1, le=1440)):
    return fetch_all("""SELECT vehicle_id, zone, idle_since, 'vehicle_idle' AS type
        FROM rt_vehicle_state WHERE status='idle'
        AND event_ts >= (SELECT max(event_ts) FROM rt_vehicle_state) - interval '30 minutes'
        AND idle_since <= (SELECT max(event_ts) FROM rt_vehicle_state) - %s * interval '1 minute'
        ORDER BY idle_since""", (idle_minutes,))


@app.get("/reports/daily")
def report_dates():
    return fetch_all("SELECT dt, count(*) AS vehicles FROM daily_vehicle_profit GROUP BY dt ORDER BY dt DESC")


@app.get("/reports/daily/{report_date}")
def daily_report(report_date: date):
    snapshot = fetch_all("""SELECT
        (SELECT jsonb_agg(to_jsonb(p) ORDER BY profit_cents NULLS LAST,vehicle_id)
         FROM daily_vehicle_profit p WHERE dt=%s) AS vehicles,
        (SELECT to_jsonb(s)-'input_fingerprint' FROM daily_report_status s WHERE dt=%s) AS publication""",
                     (report_date, report_date))
    rows = snapshot[0]['vehicles'] if snapshot else None
    if not rows:
        raise HTTPException(404, "No reconciled report for that simulated date yet")
    return {"date": report_date, "currency": "LKR", "money_unit": "cents", "vehicles": rows,
            "publication": snapshot[0]['publication'] or {"export_status": "legacy_unverified"}}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    status = pipeline_status()
    age = status["last_event_age_seconds"]
    return ("# TYPE fleet_pipeline_healthy gauge\n"
            f"fleet_pipeline_healthy {int(status['healthy'])}\n"
            "# TYPE fleet_events_ingested_total counter\n"
            f"fleet_events_ingested_total {status['rows_in']}\n"
            "# TYPE fleet_events_rejected_total counter\n"
            f"fleet_events_rejected_total {status['rows_rejected']}\n"
            "# TYPE fleet_last_event_age_seconds gauge\n"
            f"fleet_last_event_age_seconds {age if age is not None else -1}\n")
