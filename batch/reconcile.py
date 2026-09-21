import argparse
import csv
import json
import uuid
from datetime import date, datetime, time, timedelta, timezone

from psycopg2.extras import Json, execute_values

from common.db import connection, fetch_all
from common.domain import reconcile, validate_expenses
from common.logging_conf import log
from common.settings import DATA_DIR, DQ_FAILURE_THRESHOLD, vehicles


def read_events(report_date, batch_ids):
    import pyarrow.parquet as pq

    for batch_id in batch_ids:
        # Only DB-committed archives are eligible; unfinished Spark writes are ignored.
        directory = DATA_DIR / "raw" / str(batch_id) / f"dt={report_date}"
        for path in sorted(directory.glob("*.parquet")):
            for chunk in pq.ParquetFile(path).iter_batches(batch_size=2048):
                yield from chunk.to_pylist()


def run_day(report_date):
    # A session lock prevents a manual backfill from racing the scheduled run.
    day = date.fromisoformat(report_date)
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(8203, %s)", (day.toordinal(),))
        if not cur.fetchone()[0]:
            log("batch", "date_already_running", report_date=report_date)
            return False
        return _run_day(report_date)


def _run_day(report_date):
    day = date.fromisoformat(report_date)
    next_day = datetime.combine(day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    batches = fetch_all("SELECT batch_id, max_event_ts FROM pipeline_batches ORDER BY batch_id")
    timestamps = [row["max_event_ts"] for row in batches if row["max_event_ts"] is not None]
    if not timestamps or max(timestamps) < next_day:
        log("batch", "waiting_for_raw_day_boundary", report_date=report_date)
        return False
    source = DATA_DIR / "landing" / "expenses" / f"expenses_{report_date}.csv"
    run_id = str(uuid.uuid4())
    with connection() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO pipeline_runs (run_id, dt, stage, status) VALUES (%s,%s,'reconcile','running')",
                    (run_id, report_date))
    try:
        with source.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            raise ValueError("Expense file has no rows")
        valid, rejected = validate_expenses(rows, report_date, vehicles())
        if rejected:
            with connection() as conn, conn.cursor() as cur:
                execute_values(cur, """INSERT INTO dq_quarantine
                    (run_id, dt, source_file, row_num, rule_failed, raw_row) VALUES %s""",
                    [(run_id, report_date, source.name, item["row_number"], item["reason"], Json(item["row"]))
                     for item in rejected])
        if len(rejected) / len(rows) > DQ_FAILURE_THRESHOLD:
            raise ValueError(f"Data quality gate failed: {len(rejected)}/{len(rows)} invalid rows")
        output = reconcile(read_events(report_date, [row["batch_id"] for row in batches]), valid, report_date)
        with connection() as conn, conn.cursor() as cur:
            # Replace the entire day transactionally, removing stale rows after corrections.
            cur.execute("DELETE FROM daily_vehicle_profit WHERE dt = %s", (report_date,))
            if output:
                columns = list(output[0])
                execute_values(cur, "INSERT INTO daily_vehicle_profit (" + ",".join(columns) + ") VALUES %s",
                               [tuple(item[column] for column in columns) for item in output])
        report_dir = DATA_DIR / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        target = report_dir / f"profitability_{report_date}.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps({"date": report_date, "currency": "LKR", "money_unit": "cents",
                                         "run_id": run_id, "vehicles": output}, indent=2), encoding="utf-8")
        temporary.replace(target)
        with connection() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE pipeline_runs SET status='success', rows_in=%s, rows_out=%s,
                           ended_at=now() WHERE run_id=%s""", (len(rows), len(output), run_id))
        log("batch", "report_published", run_id=run_id, report_date=report_date,
            vehicles=len(output), quarantined=len(rejected))
        return True
    except Exception as exc:
        with connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE pipeline_runs SET status='failed', error=%s, ended_at=now() WHERE run_id=%s",
                        (str(exc), run_id))
        log("batch", "reconciliation_failed", run_id=run_id, report_date=report_date, error=str(exc))
        raise


def run_available():
    """Recompute closed days to incorporate corrected files and late raw events.

    This bounded classroom demo deliberately scans all landed days on every run.
    Production would use versioned input manifests and targeted backfills.
    """
    for source in sorted((DATA_DIR / "landing" / "expenses").glob("expenses_*.csv")):
        run_day(source.stem.removeprefix("expenses_"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    args = parser.parse_args()
    if args.date:
        run_day(args.date.isoformat())
    else:
        run_available()
