import hashlib
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from common.publication import export_matches
from common.domain import SIM_START
from simulators.fixtures import make_event
from streaming.sink import fingerprint


class CompletionTests(unittest.TestCase):
    def test_export_integrity_missing_modified_and_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'report.json'
            expected = hashlib.sha256(b'correct').hexdigest()
            self.assertFalse(export_matches(path, expected))
            path.write_bytes(b'correct')
            self.assertTrue(export_matches(path, expected))
            path.write_bytes(b'corrupt')
            self.assertFalse(export_matches(path, expected))
            self.assertFalse(export_matches(path, None))

    def test_timestamp_spellings_share_identity(self):
        item = make_event(1, 5, SIM_START, SIM_START)
        self.assertEqual(fingerprint(item), fingerprint(dict(item, event_ts='2026-03-01T05:30:00+05:30')))

    def _mock_conn(self, rows_per_query):
        """Build a context-manager mock connection whose cursor returns rows in sequence."""
        from unittest.mock import MagicMock
        results = list(rows_per_query)
        cur = MagicMock()
        call_count = [0]
        def fetchone():
            r = results[call_count[0]]; call_count[0] += 1; return r
        def fetchall():
            r = results[call_count[0]]; call_count[0] += 1; return r
        cur.fetchone.side_effect = fetchone
        cur.fetchall.side_effect = fetchall
        cur.__enter__ = lambda s: s
        cur.__exit__ = MagicMock(return_value=False)
        conn = MagicMock()
        conn.cursor.return_value = cur
        conn.__enter__ = lambda s: s
        conn.__exit__ = MagicMock(return_value=False)
        return conn

    @patch('api.main.export_matches', return_value=True)
    @patch('common.db.connection')
    def test_report_health_detects_historical_hole(self, mock_conn, mock_exp):
        from fastapi.testclient import TestClient
        from api.main import app
        # Queries in order: SET TRANSACTION (no fetch), summary, publication,
        # expected, exports, missing, active_run
        conn = self._mock_conn([
            {'failed_dates': 0, 'stalled_dates': 0},         # summary fetchone
            {'latest_report': date(2026, 3, 3), 'pending_exports': 0},  # publication fetchone
            {'expected_report': date(2026, 3, 3)},            # expected fetchone
            [],                                               # exports fetchall
            {'n': 1},                                         # missing fetchone
            None,                                             # active_run fetchone
        ])
        mock_conn.return_value = conn
        response = TestClient(app).get('/health/reports')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['missing_dates'], 1)

    @patch('api.main.export_matches', return_value=False)
    @patch('common.db.connection')
    def test_report_health_detects_corrupt_export(self, mock_conn, mock_exp):
        from fastapi.testclient import TestClient
        from api.main import app
        from common.publication import ALGORITHM_VERSION
        conn = self._mock_conn([
            {'failed_dates': 0, 'stalled_dates': 0},
            {'latest_report': date(2026, 3, 1), 'pending_exports': 0},
            {'expected_report': date(2026, 3, 1)},
            [{'dt': date(2026, 3, 1), 'output_sha256': 'bad', 'algorithm_version': ALGORITHM_VERSION}],
            {'n': 0},
            None,
        ])
        mock_conn.return_value = conn
        response = TestClient(app).get('/health/reports')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['invalid_exports'], 1)


class TimezoneRegressionTests(unittest.TestCase):
    """Assert that psycopg2's UTC timezone representation compares equal to
    Python's timezone.utc.  The set union in sink.bulk_serve (line 55) builds
    old_trips from psycopg2 cursor rows and compares them with datetimes from
    parse_timestamp().  If the two tzinfo objects did not compare equal for the
    same instant, valid trips would be falsely flagged as conflicts."""

    def test_psycopg2_utc_equals_python_utc(self):
        try:
            import psycopg2.tz
        except ImportError:
            self.skipTest("psycopg2 not installed")
        from common.domain import parse_timestamp
        # psycopg2 returns timestamptz as datetime with FixedOffsetTimezone(0)
        db_ts = datetime(2026, 3, 1, 0, 0, 0, tzinfo=psycopg2.tz.FixedOffsetTimezone(0))
        py_ts = parse_timestamp("2026-03-01T00:00:00Z")
        self.assertEqual(db_ts, py_ts,
            "psycopg2 UTC datetime must equal parse_timestamp() result; "
            "inequality would cause false trip conflicts in sink.bulk_serve")
        # Same instant in +05:30 must also match
        self.assertEqual(parse_timestamp("2026-03-01T05:30:00+05:30"), db_ts)

    def test_psycopg2_utc_set_union_size_one(self):
        """The set union in bulk_serve:55 must have size 1 for an exact replay."""
        try:
            import psycopg2.tz
        except ImportError:
            self.skipTest("psycopg2 not installed")
        from common.domain import parse_timestamp
        db_tuple = ("V-001", 5000, datetime(2026, 3, 1, tzinfo=psycopg2.tz.FixedOffsetTimezone(0)), "COLOMBO-01")
        py_tuple = ("V-001", 5000, parse_timestamp("2026-03-01T00:00:00Z"), "COLOMBO-01")
        self.assertEqual(len({db_tuple} | {py_tuple}), 1,
            "A replayed trip must form a size-1 set so it is not marked as conflicting")


class SparkPythonParityTests(unittest.TestCase):
    """Verify that the Spark batch path and the pure-Python reconcile() produce
    identical per-vehicle financial results for the same input data.
    This guards against the Lambda duplication risk: a formula change in one
    path without updating the other is the most likely source of a silent bug."""

    def test_spark_aggregate_matches_python_reconcile(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise unittest.SkipTest("Parity test needs pyarrow") from exc

        from common.domain import reconcile, validate_expenses
        from simulators.fixtures import make_event, expense_rows
        from common.settings import EVENT_INTERVAL_SECONDS, SIM_DAY_SECONDS

        day = "2026-03-01"
        known = {"V-001", "V-002", "V-003"}
        step = EVENT_INTERVAL_SECONDS * 86400 / SIM_DAY_SECONDS
        observations = round(SIM_DAY_SECONDS / EVENT_INTERVAL_SECONDS)
        events = [
            make_event(vehicle, tick, SIM_START + timedelta(seconds=tick * step), SIM_START)
            for tick in range(observations)
            for vehicle in range(1, 4)
        ]
        raw_costs = list(expense_rows(day, 3))
        expenses, _ = validate_expenses(raw_costs, day, known)

        with tempfile.TemporaryDirectory() as tmp:
            # Write the fixture events as Parquet in the partition layout that
            # the batch path expects: <root>/dt=<date>/<file>.parquet
            part_dir = Path(tmp) / f"dt={day}"
            part_dir.mkdir(parents=True)
            parquet_path = part_dir / "part.parquet"
            pq.write_table(pa.Table.from_pylist(events), parquet_path)

            try:
                from batch import spark_reconcile
                spark_rows, _, zone_rows = spark_reconcile.aggregate(
                    [str(parquet_path)], expenses, day, known
                )
            except ModuleNotFoundError as exc:
                raise unittest.SkipTest("Parity test needs pyspark") from exc
            finally:
                if "spark_reconcile" in locals() and spark_reconcile._session is not None:
                    spark_reconcile._session.stop()
                    spark_reconcile._session = None

        self.assertTrue(zone_rows, "Spark batch must produce a non-empty zone summary")

        python_rows = reconcile(events, expenses, day)
        spark_by_v = {r["vehicle_id"]: r for r in spark_rows}
        python_by_v = {r["vehicle_id"]: r for r in python_rows}

        self.assertEqual(set(spark_by_v), set(python_by_v),
                         "Both paths must produce the same set of vehicle IDs")
        for vehicle in sorted(known):
            with self.subTest(vehicle=vehicle):
                s, p = spark_by_v[vehicle], python_by_v[vehicle]
                self.assertEqual(s["trips"], p["trips"],
                                 f"{vehicle}: trip count mismatch")
                self.assertEqual(s["revenue_cents"], p["revenue_cents"],
                                 f"{vehicle}: revenue mismatch")
                self.assertEqual(s["profit_cents"], p["profit_cents"],
                                 f"{vehicle}: profit mismatch")
                self.assertEqual(s["is_unprofitable"], p["is_unprofitable"],
                                 f"{vehicle}: is_unprofitable mismatch")
                self.assertEqual(s["reconciliation_status"], p["reconciliation_status"],
                                 f"{vehicle}: reconciliation_status mismatch")
