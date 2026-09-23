"""Data Quality DAG — separate observability pipeline per PROJECT_PLAN.md §5.1.

Runs every 5 minutes and checks:
  1. Quarantine rate for recent expense runs (should be <= DQ_FAILURE_THRESHOLD).
  2. Dead-letter accumulation rate (should be near zero).
  3. Archive manifest integrity for every committed batch.
  4. Report export SHA-256 for every published date.

Fails the relevant Airflow task and logs a structured DQ event if a check fails.
It does not reprocess data; that is the profitability DAG's responsibility.
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
        WITH recent AS (
            SELECT run_id,dt,rows_in FROM pipeline_runs
            WHERE started_at > now() - interval '24 hours' AND rows_in IS NOT NULL
        )
        SELECT r.run_id,r.dt,r.rows_in AS total_rows,count(q.id) AS bad_rows
        FROM recent r LEFT JOIN dq_quarantine q ON q.run_id=r.run_id
        GROUP BY r.run_id,r.dt,r.rows_in
    """)
    for row in rows:
        if row["total_rows"] and row["bad_rows"] / row["total_rows"] > DQ_FAILURE_THRESHOLD:
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
    """Verify SHA-256 manifests for every committed batch."""
    from common.db import fetch_all
    from common.archive import _get_fs, digest
    from common.settings import MINIO_BUCKET
    from common.logging_conf import log

    batches = fetch_all("""
        SELECT batch_id, archive_manifest, rows_valid FROM pipeline_batches
        ORDER BY batch_id
    """)
    failed = []
    fs = _get_fs()
    for b in batches:
        manifest = b.get("archive_manifest") or {}
        if not manifest or manifest.get("rows") != b.get("rows_valid"):
            failed.append(b["batch_id"])
            continue

        root = f"{MINIO_BUCKET}/{b['batch_id']}"
        ok = True
        for item in manifest.get("files", []):
            path = f"{root}/{item['path']}"
            if not fs.exists(path) or digest(path) != item["sha256"]:
                ok = False
                break
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
        ORDER BY dt DESC
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
