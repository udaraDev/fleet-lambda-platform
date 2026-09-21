"""Real PostgreSQL/Parquet checks in a disposable schema, never the live tables."""

import csv
import json
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import pyarrow as pa
import pyarrow.parquet as pq

import batch.reconcile as batch
from common.domain import EXPENSE_FIELDS
from common.settings import DATABASE_URL
from simulators.fixtures import expense_rows, sample_events


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
        with tempfile.TemporaryDirectory(prefix="fleet-verify-") as directory:
            root = Path(directory)
            day = "2026-03-01"
            partition = root / "raw" / "1" / f"dt={day}"
            partition.mkdir(parents=True)
            events = sample_events()
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
                        (batch_id, rows_in, rows_valid, rows_rejected, max_event_ts)
                        VALUES (1,72,72,0,'2026-03-02T00:00:00Z')""")
                assert batch.run_day(day)
                original = report()
                assert len(original) == 4
                assert [row["trips"] for row in original] == [2, 2, 2, 0]
                assert original[3]["profit_cents"] == -1380000
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
                assert len(reduced) == 3, "Corrected report retained a stale vehicle"
                assert reduced[0]["reconciliation_status"] == "missing_expenses"
                assert reduced[0]["profit_cents"] is None

                with connection() as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_lock(8203, %s)", (original[0]["dt"].toordinal(),))
                    assert batch.run_day(day) is False, "Concurrent same-day execution was allowed"
            print(json.dumps({"result": "passed", "checks": [
                "day readiness", "duplicate trip replay", "expense-only losses", "identical rerun",
                "corrected expense", "bad-data quarantine", "preserve last good report",
                "stale row removal", "unknown missing cost", "concurrent-run lock"
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
