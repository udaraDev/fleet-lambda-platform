from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response, HTMLResponse

from common.db import fetch_all
from common.logging_conf import log
from common.settings import NO_DATA_SECONDS, RAW_ARCHIVE_LAG_MINUTES, VEHICLE_COUNT
from common.settings import DATA_DIR
from common.publication import ALGORITHM_VERSION, manifest_matches

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
    summary = fetch_all("""WITH event_times AS (
        SELECT (SELECT max(event_ts) FROM stream_event_keys) AS live_event_ts,
               (SELECT max(max_event_ts) FROM pipeline_batches) AS raw_event_ts
    ) SELECT
        (SELECT GREATEST(EXTRACT(EPOCH FROM (now()-max(accepted_at))),
            (SELECT EXTRACT(EPOCH FROM (now()-started_at)) FROM simulation_clock WHERE id=1)
            - EXTRACT(EPOCH FROM (max(event_ts)-timestamptz '2026-03-01 00:00:00+00'))
              * (SELECT day_seconds FROM simulation_clock WHERE id=1) / 86400.0)
         FROM stream_event_keys) AS last_event_age_seconds,
        live_event_ts,raw_event_ts,
        GREATEST(EXTRACT(EPOCH FROM (live_event_ts-raw_event_ts)),0) AS raw_archive_lag_seconds,
        COALESCE(sum(rows_in), 0) AS rows_in,
        COALESCE(sum(rows_rejected), 0) AS rows_rejected
        FROM pipeline_batches CROSS JOIN event_times
        GROUP BY live_event_ts,raw_event_ts""")[0]
    age = summary["last_event_age_seconds"]
    raw_lag = summary.get("raw_archive_lag_seconds")
    fresh = age is not None and age <= NO_DATA_SECONDS
    archive_current = raw_lag is not None and raw_lag <= RAW_ARCHIVE_LAG_MINUTES * 60
    healthy = fresh and archive_current
    status = "healthy" if healthy else "no_recent_data" if not fresh else "raw_archive_lag"
    return {"status": status, "healthy": healthy,
            "threshold_real_seconds": NO_DATA_SECONDS,
            "raw_archive_lag_threshold_seconds": RAW_ARCHIVE_LAG_MINUTES * 60,
            **summary}


@app.get("/health/pipeline")
def health_pipeline():
    try:
        status = pipeline_status()
        # Decimal from PostgreSQL is converted explicitly for this Response.
        status = {
            key: float(value) if isinstance(value, Decimal)
            else value.isoformat() if hasattr(value, "isoformat")
            else value
            for key, value in status.items()
        }
        return JSONResponse(status, status_code=200 if status["healthy"] else 503)
    except Exception:
        log("api", "database_unavailable")
        return JSONResponse({"status": "database_unavailable", "healthy": False}, status_code=503)


@app.get("/health/reports")
def health_reports():
    """Batch health is separate from live ingestion and financial completeness."""
    from common.db import connection
    from psycopg2.extras import RealDictCursor
    try:
        with connection() as conn:
            # One repeatable-read snapshot prevents split-brain responses when a
            # reconciliation commit lands between queries.
            with conn.cursor() as _iso:
                _iso.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""WITH latest AS (
                    SELECT DISTINCT ON (dt) dt,status,started_at FROM pipeline_runs ORDER BY dt,started_at DESC
                ) SELECT count(*) FILTER (WHERE status='failed') AS failed_dates,
                    count(*) FILTER (WHERE status='running' AND started_at < now()-interval '20 minutes') AS stalled_dates
                    FROM latest""")
                summary = dict(cur.fetchone())
                cur.execute("""SELECT max(dt) FILTER (WHERE export_status='published') AS latest_report,
                    count(*) FILTER (WHERE export_status <> 'published') AS pending_exports
                    FROM daily_report_status""")
                publication = dict(cur.fetchone())
                cur.execute("""SELECT max(max_event_ts)::date - 1 AS expected_report,
                    max(max_event_ts) AS raw_event_ts,
                    (SELECT max(event_ts) FROM stream_event_keys) AS live_event_ts
                    FROM pipeline_batches""")
                layer_times = dict(cur.fetchone())
                expected = layer_times["expected_report"]
                raw_lag_seconds = None
                if layer_times.get("raw_event_ts") and layer_times.get("live_event_ts"):
                    raw_lag_seconds = max(0.0, (
                        layer_times["live_event_ts"] - layer_times["raw_event_ts"]
                    ).total_seconds())
                cur.execute("SELECT dt,export_manifest,algorithm_version FROM daily_report_status WHERE export_status='published'")
                exports = [dict(r) for r in cur.fetchall()]
                cur.execute("""SELECT count(*) AS n FROM generate_series(date '2026-03-01',
                    %s::date,interval '1 day') AS d(dt) LEFT JOIN daily_report_status s ON s.dt=d.dt
                    WHERE s.dt IS NULL""", (expected,))
                missing = cur.fetchone()["n"]
                # Grace period: check if Airflow is actively computing the expected date
                # so we return 'processing' instead of a false 503 at every day boundary.
                cur.execute("""SELECT 1 FROM pipeline_runs
                    WHERE dt=%s AND status='running'
                    AND started_at > now()-interval '3 minutes'
                    LIMIT 1""", (expected,))
                active_run = cur.fetchone() is not None
        # File-system checks are outside the DB snapshot (no DB interaction).
        bad_exports = sum(not manifest_matches(DATA_DIR / "reports", r["export_manifest"])
                          for r in exports)
        old_versions = sum(r['algorithm_version'] != ALGORITHM_VERSION for r in exports)
        healthy = (not summary["failed_dates"] and not summary["stalled_dates"] and
                   not publication["pending_exports"] and not bad_exports and not old_versions
                   and not missing and expected is not None and
                   publication["latest_report"] is not None and
                   publication["latest_report"] >= expected and
                   raw_lag_seconds is not None and
                   raw_lag_seconds <= RAW_ARCHIVE_LAG_MINUTES * 60)
        # The accelerated simulator can close a date while Spark is still scanning
        # the archive and before that date's pipeline_runs row is inserted. Treat one
        # consecutive unpublished date as bounded catch-up; two missed dates still
        # become a real degradation even if Airflow metadata is unavailable here.
        one_date_catchup = (
            missing == 1 and expected is not None and
            publication["latest_report"] == expected - timedelta(days=1)
        )
        # Return 'processing' when the only failure signal is the brand-new expected
        # date not yet published and reconciliation is active or only one date behind.
        if (not healthy and (active_run or one_date_catchup) and not summary["failed_dates"] and
                not summary["stalled_dates"] and not bad_exports and not old_versions and
                raw_lag_seconds is not None and
                raw_lag_seconds <= RAW_ARCHIVE_LAG_MINUTES * 60):
            from fastapi.encoders import jsonable_encoder
            return JSONResponse(jsonable_encoder({
                "healthy": True, "status": "processing",
                "expected_report": expected, "missing_dates": missing,
                "invalid_exports": 0, "outdated_dates": 0,
                "algorithm_version": ALGORITHM_VERSION,
                "raw_archive_lag_seconds": raw_lag_seconds,
                "raw_archive_lag_threshold_seconds": RAW_ARCHIVE_LAG_MINUTES * 60,
                **summary, **publication}), status_code=200)
        from fastapi.encoders import jsonable_encoder
        return JSONResponse(jsonable_encoder({
            "healthy": healthy, "status": "healthy" if healthy else "degraded",
            "expected_report": expected, "missing_dates": missing,
            "invalid_exports": bad_exports, "outdated_dates": old_versions,
            "algorithm_version": ALGORITHM_VERSION,
            "raw_archive_lag_seconds": raw_lag_seconds,
            "raw_archive_lag_threshold_seconds": RAW_ARCHIVE_LAG_MINUTES * 60,
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
def zones(window: int = Query(default=15, ge=1, le=1440)):
    """Return Spark event-time window metrics over the requested simulated minutes."""
    return fetch_all("""WITH reference AS (
            SELECT max(window_start) AS ts FROM rt_zone_metrics
        ), selected AS (
            SELECT m.* FROM rt_zone_metrics m, reference
            WHERE m.window_start > reference.ts - (%s * interval '1 minute')
              AND m.window_start <= reference.ts
        )
        SELECT zone,
            round(avg(active_vehicles))::integer AS active_vehicles,
            round(avg(reporting_vehicles))::integer AS reporting_vehicles,
            avg(idle_ratio)::float AS idle_ratio,
            sum(trips)::bigint AS trips_last_hour,
            sum(earnings_cents)::bigint AS earnings_cents,
            min(window_start) AS window_start,
            max(window_start) + interval '1 minute' AS window_end,
            %s::integer AS window_minutes
        FROM selected GROUP BY zone ORDER BY zone""", (window, window))


@app.get("/metrics/time-of-day")
def time_of_day(report_date: date):
    return fetch_all("""SELECT zone, time_of_day_bucket, count(*) AS trips, sum(fare_cents) AS earnings_cents
        FROM completed_trips WHERE completed_at >= %s::date
        AND completed_at < %s::date + interval '1 day'
        GROUP BY zone, time_of_day_bucket ORDER BY zone, time_of_day_bucket""", (report_date, report_date))


@app.get("/alerts/active")
def alerts(idle_minutes: int = Query(default=15, ge=1, le=1440)):
    return fetch_all("""SELECT vehicle_id, alert_type AS type, raised_at, payload AS details
        FROM rt_alerts WHERE resolved_at IS NULL
          AND COALESCE(rt_alerts.idle_minutes, 0) >= %s
        ORDER BY raised_at""", (idle_minutes,))


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


@app.get("/reports/daily/{report_date}/unprofitable")
def unprofitable_vehicles(report_date: date):
    """Return vehicles that are loss-making for a given simulated date, worst first.

    A vehicle appears here only when its profit is both known (expenses present,
    no telemetry conflict) and negative.  Vehicles with unknown profit due to
    missing or conflicting data are excluded — unknown is not the same as zero.
    """
    rows = fetch_all("""
        SELECT vehicle_id, trips, revenue_cents,
               fuel_cents, maintenance_cents, profit_cents, margin_pct,
               reconciliation_status
        FROM daily_vehicle_profit
        WHERE dt = %s AND is_unprofitable = true
        ORDER BY profit_cents ASC NULLS LAST
    """, (report_date,))
    if not rows:
        raise HTTPException(404, "No vehicles with confirmed losses for that simulated date")
    return {"date": report_date, "currency": "LKR", "money_unit": "cents",
            "count": len(rows), "vehicles": rows}




@app.get("/metrics")
def metrics():
    from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST

    status = pipeline_status()
    age = status["last_event_age_seconds"]
    registry = CollectorRegistry()
    values = (
        ("fleet_pipeline_healthy", "Whether accepted telemetry is fresh", int(status["healthy"])),
        ("fleet_events_ingested_total", "Total Kafka rows observed", status["rows_in"]),
        ("fleet_events_rejected_total", "Total streaming rows rejected", status["rows_rejected"]),
        ("fleet_last_event_age_seconds", "Real-time age of the latest accepted event", age if age is not None else -1),
        ("fleet_raw_archive_lag_seconds", "Simulated event-time lead of serving over raw archive",
         status.get("raw_archive_lag_seconds") if status.get("raw_archive_lag_seconds") is not None else -1),
    )
    for name, description, value in values:
        Gauge(name, description, registry=registry).set(float(value))
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
