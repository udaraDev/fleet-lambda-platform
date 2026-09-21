"""Real PostgreSQL/Parquet checks in a disposable schema, never the live tables."""

import csv
import json
import tempfile
import uuid
from datetime import timedelta
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import pyarrow as pa
import pyarrow.parquet as pq

import batch.reconcile as batch
from common.domain import EXPENSE_FIELDS, SIM_START
from common.archive import build_manifest
from psycopg2.extras import Json
from common.settings import DATABASE_URL
from simulators.fixtures import expense_rows, make_event


def verify():
    schema = "verify_reconcile_" + uuid.uuid4().hex
    admin = psycopg2.connect(DATABASE_URL, connect_timeout=5)
    admin.autocommit = True

    @contextmanager
    def connection():
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=5, options="-c timezone=UTC")
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
                yield conn
        finally:
            conn.close()

    def fetch_all(query, params=()):
        with connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    created = False
    try:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            created = True
        with connection() as conn, conn.cursor() as cur:
            cur.execute((Path(__file__).resolve().parents[1] / "sql" / "001_schema.sql").read_text())
            cur.execute((Path(__file__).resolve().parents[1] / "sql" / "002_integrity.sql").read_text())
        from streaming.sink import bulk_serve
        first = dict(make_event(1, 5, SIM_START + timedelta(days=3), SIM_START), time_of_day_bucket="night")
        with connection() as conn, conn.cursor() as cur:
            assert bulk_serve(cur, [first], 10) == 0
            assert bulk_serve(cur, [first], 11) == 0
            cur.execute("SELECT count(*) FROM completed_trips")
            assert cur.fetchone()[0] == 1, "Cross-batch replay double counted a trip"
            assert bulk_serve(cur, [dict(first, fare_cents=1)], 12) > 0
            cur.execute("SELECT count(*) FROM completed_trips")
            assert cur.fetchone()[0] == 0, "Conflicting fare remained authoritative"
            good = dict(make_event(2, 11, SIM_START + timedelta(days=3), SIM_START), time_of_day_bucket="night")
            assert bulk_serve(cur, [good], 13) == 0
            cur.execute("SELECT count(*) FROM completed_trips")
            assert cur.fetchone()[0] == 1, "Conflict prevented subsequent good data"
        from batch.spark_reconcile import aggregate
        empty, coverage = aggregate([], [], "2026-03-10", ["V-001"])
        assert empty[0]["profit_cents"] is None and not coverage["V-001"]["complete"]
        with tempfile.TemporaryDirectory(prefix="fleet-verify-") as directory:
            root = Path(directory)
            day = "2026-03-01"
            partition = root / "raw" / "1" / f"dt={day}"
            partition.mkdir(parents=True)
            events = [make_event(n, tick, SIM_START + timedelta(seconds=tick * 576), SIM_START)
                      for tick in range(150) for n in range(1, 5)]
            # Replay the same completions within the committed archive.
            pq.write_table(pa.Table.from_pylist(events + events), partition / "part.parquet")
            source = root / "landing" / "expenses" / f"expenses_{day}.csv"
            source.parent.mkdir(parents=True)

            def write_expenses(rows):
                with source.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=EXPENSE_FIELDS)
                    writer.writeheader()
                    writer.writerows(rows)

            def report():
                return fetch_all("SELECT * FROM daily_vehicle_profit ORDER BY vehicle_id")

            expenses = list(expense_rows(day, 4))
            write_expenses(expenses)
            with patch.multiple(batch, DATA_DIR=root, connection=connection, fetch_all=fetch_all,
                                vehicles=lambda: [f"V-{n:03d}" for n in range(1, 5)],
                                DQ_FAILURE_THRESHOLD=0.05):
                assert batch.run_day(day) is False, "Published before raw day boundary"
                with connection() as conn, conn.cursor() as cur:
                    cur.execute("""INSERT INTO pipeline_batches
                        (batch_id, rows_in, rows_valid, rows_rejected, max_event_ts, archive_manifest)
                        VALUES (1,1200,1200,0,'2026-03-02T00:00:00Z',%s)""",
                        (Json(build_manifest(root / 'raw' / '1', 1200)),))
                assert batch.run_day(day)
                original = report()
                assert len(original) == 4
                assert [row["trips"] for row in original] == [25, 25, 25, 0]
                assert original[3]["profit_cents"] == -1380000
                from common.domain import validate_expenses
                clean_costs, _ = validate_expenses(list(expense_rows(day, 4)), day, batch.vehicles())
                edge_events = [e for e in events if not (e["vehicle_id"] == "V-001" and e["event_id"].endswith("0000000050"))]
                conflicting = next(e for e in events if e["vehicle_id"] == "V-002" and e["trip_completed"])
                edge_events.append(dict(conflicting, fare_cents=1))
                edge_path = root / "edge-cases.parquet"
                pq.write_table(pa.Table.from_pylist(edge_events), edge_path)
                edge_output, _ = aggregate([str(edge_path)], clean_costs, day, batch.vehicles())
                assert edge_output[0]["reconciliation_status"] == "incomplete_telemetry"
                assert edge_output[1]["reconciliation_status"] == "conflicting_events"
                assert edge_output[2]["reconciliation_status"] == "complete"
                assert edge_output[3]["profit_cents"] == -1380000
                assert batch.run_day(day)
                assert report() == original, "Rerun changed identical results"

                expenses[0]["fuel_cost"] = "1900.00"
                write_expenses(expenses)
                assert batch.run_day(day)
                corrected = report()
                assert corrected[0]["profit_cents"] == original[0]["profit_cents"] - 10000
                assert corrected[1:] == original[1:]

                target = root / "reports" / f"profitability_{day}.json"
                last_good_export = target.read_bytes()
                expenses[0]["fuel_cost"] = "-1.00"
                write_expenses(expenses)
                try:
                    batch.run_day(day)
                except ValueError as exc:
                    assert "Data quality gate failed" in str(exc)
                else:
                    raise AssertionError("Invalid expense file was accepted")
                assert report() == corrected, "Quality failure replaced good database output"
                assert target.read_bytes() == last_good_export, "Quality failure replaced good export"
                assert fetch_all("SELECT count(*) AS n FROM dq_quarantine")[0]["n"] == 1
                assert fetch_all("SELECT count(*) AS n FROM pipeline_runs WHERE status='failed'")[0]["n"] == 1

                # Omit one trip vehicle's cost and the cost-only vehicle entirely.
                write_expenses(expenses[1:3])
                assert batch.run_day(day)
                reduced = report()
                assert len(reduced) == 4, "Registered vehicles must remain visible"
                assert reduced[0]["reconciliation_status"] == "missing_expenses"
                assert reduced[0]["profit_cents"] is None
                assert reduced[3]["reconciliation_status"] == "missing_expenses"

                # An absent committed file must not replace the last report.
                archived = partition / "part.parquet"
                moved = partition / "part.hidden"
                archived.rename(moved)
                try:
                    try:
                        batch.run_day(day)
                    except ValueError as exc:
                        assert "Missing or changed" in str(exc)
                    else:
                        raise AssertionError("Missing archive was silently accepted")
                    assert report() == reduced
                finally:
                    moved.rename(archived)

                write_expenses(list(expense_rows(day, 4)))
                last_export = target.read_bytes()
                with patch("pathlib.Path.replace", side_effect=OSError("simulated export failure")):
                    try:
                        batch.run_day(day)
                    except OSError:
                        pass
                    else:
                        raise AssertionError("Export failure was hidden")
                assert target.read_bytes() == last_export
                assert fetch_all("SELECT export_status FROM daily_report_status WHERE dt=%s", (day,))[0]["export_status"] == "pending"
                assert batch.run_day(day)
                published = fetch_all("SELECT run_id,export_status FROM daily_report_status WHERE dt=%s", (day,))[0]
                assert published["export_status"] == "published"
                assert json.loads(target.read_text())["run_id"] == str(published["run_id"])

                with connection() as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_lock(8203, %s)", (original[0]["dt"].toordinal(),))
                    assert batch.run_day(day) is False, "Concurrent same-day execution was allowed"
            # Exercise the actual Spark ingestion callback against only this schema/temp root.
            from datetime import datetime, timezone
            from batch.spark_reconcile import session
            from streaming.validation import validation_error
            from pyspark.sql import functions as F
            import streaming.job as job
            event = make_event(3, 17, SIM_START, SIM_START)
            schema_type = session().createDataFrame([event]).schema
            inputs = [json.dumps(event), json.dumps(dict(event, vehicle_id="UNKNOWN")), "broken-json",
                      json.dumps(dict(event, event_id="future-event", event_ts="2099-01-01T00:00:00Z"))]
            frame = session().createDataFrame([(raw, 0, i) for i, raw in enumerate(inputs)],
                                              "raw string, partition int, offset long")
            frame = frame.withColumn("event", F.from_json("raw", schema_type)).withColumn("error", validation_error())
            with patch.multiple(job, DATA_DIR=root, connection=connection,
                                clock_start=lambda: datetime.now(timezone.utc)):
                job.process_batch(frame, 20)
                job.process_batch(frame, 20)
            ingestion = fetch_all("SELECT rows_valid,rows_rejected FROM pipeline_batches WHERE batch_id=20")[0]
            assert ingestion == {"rows_valid": 1, "rows_rejected": 3}, ingestion
            print(json.dumps({"result": "passed", "checks": [
                "day readiness", "duplicate trip replay", "expense-only losses", "identical rerun",
                "corrected expense", "bad-data quarantine", "preserve last good report",
                "registered vehicle visibility", "unknown missing cost", "concurrent-run lock", "missing archive safety",
                "bulk cross-batch replay", "conflict recovery", "missing telemetry", "native stream validation",
                "stream commit retry", "partial-day gap", "Spark conflict detection", "export failure recovery"
            ]}, indent=2))
    finally:
        try:
            if created:
                # Only the exact UUID-named schema created above is removed.
                with admin.cursor() as cur:
                    cur.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        finally:
            admin.close()


if __name__ == "__main__":
    verify()
