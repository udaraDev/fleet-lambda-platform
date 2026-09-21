import argparse
import csv
import hashlib
import io
import json
import uuid
from datetime import date, datetime, time, timedelta, timezone

from psycopg2.extras import Json, execute_values

from common.db import connection, fetch_all
from common.domain import SIM_START, validate_expenses
from common.archive import verified_paths
from common.logging_conf import log
from common.settings import DATA_DIR, DQ_FAILURE_THRESHOLD, EVENT_INTERVAL_SECONDS, SIM_DAY_SECONDS, MAX_CHANGED_DATES_PER_RUN, vehicles


def read_events(report_date, batch_ids):
    import pyarrow.parquet as pq

    for batch_id in batch_ids:
        # Only DB-committed archives are eligible; unfinished Spark writes are ignored.
        directory = DATA_DIR / "raw" / str(batch_id) / f"dt={report_date}"
        for path in sorted(directory.glob("*.parquet")):
            for chunk in pq.ParquetFile(path).iter_batches(batch_size=2048):
                yield from chunk.to_pylist()


def run_day(report_date, batches=None):
    # A session lock prevents a manual backfill from racing the scheduled run.
    day = date.fromisoformat(report_date)
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(8203, %s)", (day.toordinal(),))
        if not cur.fetchone()[0]:
            log("batch", "date_already_running", report_date=report_date)
            return False
        return _run_day(report_date, batches)


def _run_day(report_date, batches=None):
    day = date.fromisoformat(report_date)
    next_day = datetime.combine(day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    if batches is None:
        batches = fetch_all("SELECT batch_id, max_event_ts, rows_valid, archive_manifest FROM pipeline_batches ORDER BY batch_id")
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
        paths = verified_paths(DATA_DIR, batches, report_date)
        contents = source.read_bytes()
        conflicts = fetch_all("SELECT event_id,reason,payload FROM stream_conflicts WHERE dt=%s ORDER BY batch_id,event_id,reason", (report_date,))
        fingerprint = hashlib.sha256(contents + json.dumps({
            "files": [(b["batch_id"], f) for b in batches for f in b["archive_manifest"]["files"]
                      if f["path"].startswith(f"dt={report_date}/")],
            "version": 3, "vehicles": sorted(vehicles()), "interval": EVENT_INTERVAL_SECONDS,
            "day_seconds": SIM_DAY_SECONDS, "dq_threshold": DQ_FAILURE_THRESHOLD,
            "conflicts": conflicts,
        }, sort_keys=True).encode()).hexdigest()
        previous = fetch_all("SELECT input_fingerprint, export_status FROM daily_report_status WHERE dt=%s", (report_date,))
        if previous and previous[0]["input_fingerprint"] == fingerprint and previous[0]["export_status"] == "published":
            with connection() as conn, conn.cursor() as cur:
                cur.execute("UPDATE pipeline_runs SET status='unchanged', ended_at=now() WHERE run_id=%s", (run_id,))
            return "unchanged"
        with io.StringIO(contents.decode("utf-8"), newline="") as handle:
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
        from batch.spark_reconcile import aggregate
        output, coverage = aggregate(paths, valid, report_date, vehicles())
        conflicted_vehicles = {row["payload"].get("vehicle_id") for row in conflicts}
        for row in output:
            if row["vehicle_id"] in conflicted_vehicles:
                row.update(reconciliation_status="conflicting_events", profit_cents=None,
                           margin_pct=None, is_unprofitable=None)
                coverage[row["vehicle_id"]]["conflict"] = True
        with connection() as conn, conn.cursor() as cur:
            # Replace the entire day transactionally, removing stale rows after corrections.
            cur.execute("DELETE FROM daily_vehicle_profit WHERE dt = %s", (report_date,))
            if output:
                columns = list(output[0])
                execute_values(cur, "INSERT INTO daily_vehicle_profit (" + ",".join(columns) + ") VALUES %s",
                               [tuple(item[column] for column in columns) for item in output])
            cur.execute("""INSERT INTO daily_report_status (dt,run_id,input_fingerprint,coverage,export_status)
                VALUES (%s,%s,%s,%s,'pending') ON CONFLICT(dt) DO UPDATE SET
                run_id=EXCLUDED.run_id,input_fingerprint=EXCLUDED.input_fingerprint,
                coverage=EXCLUDED.coverage,export_status='pending',updated_at=now()""",
                (report_date, run_id, fingerprint, Json(coverage)))
        report_dir = DATA_DIR / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        target = report_dir / f"profitability_{report_date}.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps({"date": report_date, "currency": "LKR", "money_unit": "cents",
                                         "run_id": run_id, "coverage": coverage, "vehicles": output}, indent=2), encoding="utf-8")
        temporary.replace(target)
        with connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE daily_report_status SET export_status='published',updated_at=now() WHERE dt=%s AND run_id=%s",
                        (report_date, run_id))
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
    dates = {source.stem.removeprefix("expenses_") for source in
             (DATA_DIR / "landing" / "expenses").glob("expenses_*.csv")}
    batches = fetch_all("SELECT batch_id, max_event_ts, rows_valid, archive_manifest FROM pipeline_batches ORDER BY batch_id")
    boundary = max((b["max_event_ts"] for b in batches if b.get("max_event_ts")), default=None)
    if boundary:
        day = SIM_START.date()
        while day < boundary.date():
            dates.add(day.isoformat())
            day += timedelta(days=1)
    failed = []
    changed = 0
    # Publish current reports before backfills; one consistent input snapshot per run.
    for report_date in sorted(dates, reverse=True):
        try:
            result = run_day(report_date, batches=batches)
            changed += int(result is True)
            if changed >= MAX_CHANGED_DATES_PER_RUN:
                log("batch", "backfill_budget_reached", changed_dates=changed,
                    message_detail="Remaining dates are deferred to the next scheduled run")
                break
        except Exception as exc:
            failed.append(report_date)
            log("batch", "date_failed_continuing", report_date=report_date, error=str(exc))
    if failed:
        raise ValueError("Failed report dates: " + ", ".join(failed))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    args = parser.parse_args()
    if args.date:
        run_day(args.date.isoformat())
    else:
        run_available()
