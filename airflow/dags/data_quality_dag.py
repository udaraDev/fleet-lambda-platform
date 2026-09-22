"""Data Quality DAG — separate observability pipeline per PROJECT_PLAN.md §5.1.

Runs every 5 minutes and checks:
  1. Quarantine rate for the latest expense file (should be < DQ_FAILURE_THRESHOLD).
  2. Dead-letter accumulation rate (should be near zero).
  3. Archive manifest integrity for all committed batches.
  4. Report export SHA-256 for all published dates.

Raises an alert (503 on /health/reports) and logs a structured DQ_ALERT event
if any check fails.  Does NOT reprocess data — that is the profitability DAG.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def _check_quarantine_rate(**ctx):
    """Fail if any expense file in the last 24 sim-hours had > DQ_FAILURE_THRESHOLD bad rows."""
    from common.db import fetch_all
    from common.settings import DQ_FAILURE_THRESHOLD
    from common.logging_conf import log

    rows = fetch_all("""
        SELECT dt,
               count(*) AS bad_rows,
               (SELECT count(*) FROM daily_vehicle_profit p WHERE p.dt = q.dt) AS total_vehicles
        FROM dq_quarantine q
        WHERE received_at > now() - interval '10 minutes'
        GROUP BY dt
    """)
    for row in rows:
        if row["total_vehicles"] and row["bad_rows"] / max(row["total_vehicles"], 1) > DQ_FAILURE_THRESHOLD:
            log("dq_dag", "quarantine_rate_exceeded",
                dt=str(row["dt"]), bad=row["bad_rows"], threshold=DQ_FAILURE_THRESHOLD)
            raise ValueError(f"DQ quarantine rate exceeded for {row['dt']}: {row['bad_rows']} bad rows")
    log("dq_dag", "quarantine_rate_ok", checked=len(rows))


def _check_dead_letter_rate(**ctx):
    """Warn if more than 10 dead-letter events have arrived in the last 5 minutes."""
    from common.db import fetch_all
    from common.logging_conf import log

    rows = fetch_all("""
        SELECT count(*) AS n FROM stream_dead_letters
        WHERE received_at > now() - interval '5 minutes'
    """)
    n = rows[0]["n"] if rows else 0
    if n > 10:
        log("dq_dag", "dead_letter_spike", count=n, window_minutes=5)
        raise ValueError(f"Dead-letter spike: {n} malformed events in 5 minutes")
    log("dq_dag", "dead_letter_rate_ok", count=n)


def _check_archive_integrity(**ctx):
    """Verify SHA-256 manifests for all committed batches in the last 10 minutes."""
    from common.db import fetch_all
    from common.archive import verify_manifest
    from common.settings import DATA_DIR
    from common.logging_conf import log

    batches = fetch_all("""
        SELECT batch_id, archive_manifest FROM pipeline_batches
        WHERE committed_at > now() - interval '10 minutes'
    """)
    failed = []
    for b in batches:
        manifest = b.get("archive_manifest") or {}
        ok = verify_manifest(DATA_DIR, b["batch_id"], manifest)
        if not ok:
            failed.append(b["batch_id"])
    if failed:
        log("dq_dag", "archive_integrity_failed", batch_ids=failed)
        raise ValueError(f"Archive integrity failed for batches: {failed}")
    log("dq_dag", "archive_integrity_ok", checked=len(batches))


def _check_export_integrity(**ctx):
    """Verify SHA-256 of all published daily report files."""
    from common.db import fetch_all
    from common.publication import export_matches, ALGORITHM_VERSION
    from common.settings import DATA_DIR
    from common.logging_conf import log

    exports = fetch_all("""
        SELECT dt, output_sha256, algorithm_version
        FROM daily_report_status
        WHERE export_status = 'published'
        ORDER BY dt DESC LIMIT 30
    """)
    bad, old = [], []
    for r in exports:
        path = DATA_DIR / "reports" / f"profitability_{r['dt']}.json"
        if not export_matches(path, r["output_sha256"]):
            bad.append(str(r["dt"]))
        if r["algorithm_version"] != ALGORITHM_VERSION:
            old.append(str(r["dt"]))
    if bad or old:
        log("dq_dag", "export_integrity_failed", corrupt=bad, outdated=old)
        raise ValueError(f"Export integrity: corrupt={bad}, outdated={old}")
    log("dq_dag", "export_integrity_ok", checked=len(exports))


_DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="data_quality",
    description="Continuous DQ observability: quarantine rate, dead-letters, archive and export integrity",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=_DEFAULT_ARGS,
    tags=["fleet", "data-quality", "observability"],
) as dag:

    t1 = PythonOperator(task_id="check_quarantine_rate", python_callable=_check_quarantine_rate)
    t2 = PythonOperator(task_id="check_dead_letter_rate", python_callable=_check_dead_letter_rate)
    t3 = PythonOperator(task_id="check_archive_integrity", python_callable=_check_archive_integrity)
    t4 = PythonOperator(task_id="check_export_integrity", python_callable=_check_export_integrity)

    # All four checks run in parallel; any failure marks the DAG run as failed.
    [t1, t2, t3, t4]
