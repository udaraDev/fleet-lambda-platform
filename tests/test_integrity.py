import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from common.archive import build_manifest, verified_paths
from common.domain import SIM_START
from simulators.fixtures import make_event
from streaming.sink import fingerprint
import batch.reconcile as batch


class IntegrityTests(unittest.TestCase):
    def test_unchanged_verified_report_supersedes_recovered_orphan(self):
        from datetime import datetime, timezone
        from unittest.mock import MagicMock

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expenses = root / "landing" / "expenses"
            expenses.mkdir(parents=True)
            (expenses / "expenses_2026-03-01.csv").write_text(
                "vehicle_id,fuel_cents,maintenance_cents\nV-001,1,1\n", encoding="utf-8")
            target = root / "reports" / "profitability_2026-03-01.json"
            target.parent.mkdir()
            target.write_text("{}", encoding="utf-8")
            conn, cur = MagicMock(), MagicMock()
            conn.__enter__.return_value = conn
            conn.cursor.return_value.__enter__.return_value = cur
            with patch.object(batch, "DATA_DIR", root), \
                    patch.object(batch, "export_matches", return_value=True), \
                    patch.object(batch.hashlib, "sha256") as digest_mock, \
                    patch.object(batch, "fetch_all", side_effect=[[], [{
                        "input_fingerprint": "same", "export_status": "published",
                        "output_sha256": "hash"}]]), \
                    patch.object(batch, "connection", return_value=conn):
                digest_mock.return_value.hexdigest.return_value = "same"
                result = batch._run_day("2026-03-01", [{
                    "batch_id": 1, "max_event_ts": datetime(2026, 3, 2, tzinfo=timezone.utc),
                    "rows_valid": 0, "archive_manifest": {"files": []}}])
            self.assertEqual(result, "unchanged")
            self.assertTrue(any("status='superseded'" in call.args[0]
                                for call in cur.execute.call_args_list))
    @patch("common.archive.MINIO_BUCKET")
    @patch("common.archive._get_fs")
    def test_missing_manifest_fails_closed(self, mock_get_fs, mock_bucket):
        with self.assertRaisesRegex(ValueError, "no archive manifest"):
            verified_paths([{"batch_id": 1}], "2026-03-01")

    @patch("common.archive.MINIO_BUCKET")
    @patch("common.archive._get_fs")
    def test_empty_committed_batch_is_legitimate(self, mock_get_fs, mock_bucket):
        import fsspec
        mock_get_fs.return_value = fsspec.filesystem("memory")
        mock_bucket.__str__.return_value = "mem-bucket"
        manifest = build_manifest(1, 0)
        self.assertEqual(verified_paths([
            {"batch_id": 1, "rows_valid": 0, "archive_manifest": manifest}], "2026-03-01"), [])

    @patch("common.archive.MINIO_BUCKET")
    @patch("common.archive._get_fs")
    def test_missing_nonempty_archive_cannot_be_baselined(self, mock_get_fs, mock_bucket):
        import fsspec
        mock_get_fs.return_value = fsspec.filesystem("file")
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).as_posix()
            mock_bucket.__str__.return_value = root
            with self.assertRaisesRegex(ValueError, "row count mismatch"):
                build_manifest(1, 1)

    @patch("common.archive.MINIO_BUCKET")
    @patch("common.archive._get_fs")
    def test_missing_file_rejected(self, mock_get_fs, mock_bucket):
        import fsspec
        mock_get_fs.return_value = fsspec.filesystem("memory")
        mock_bucket.__str__.return_value = "mem-bucket"
        manifest = {"version": 1, "rows": 1, "files": [
            {"path": "dt=2026-03-01/part.parquet", "sha256": "missing", "rows": 1}]}
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ValueError, "Missing or changed"):
            verified_paths([{"batch_id": 1, "rows_valid": 1,
                                      "archive_manifest": manifest}], "2026-03-01")

    def test_one_bad_date_does_not_starve_later_dates(self):
        paths = [Path("expenses_2026-03-01.csv"), Path("expenses_2026-03-02.csv")]
        with patch("pathlib.Path.glob", return_value=paths), patch.object(batch, "fetch_all", return_value=[{"latest": None}]), \
                patch.object(batch, "run_day", side_effect=[ValueError("bad costs"), True]) as run:
            with self.assertRaisesRegex(ValueError, "2026-03-02"):
                batch.run_available()
            self.assertEqual([c.args[0] for c in run.call_args_list], ["2026-03-02", "2026-03-01"])

    def test_retry_ingest_time_does_not_change_business_identity(self):
        item = make_event(1, 5, SIM_START, SIM_START)
        retry = dict(item, ingest_ts="2026-09-21T00:00:00Z")
        self.assertEqual(fingerprint(item), fingerprint(retry))
        self.assertNotEqual(fingerprint(item), fingerprint(dict(item, fare_cents=1)))

    def test_backfill_budget_defers_older_changed_dates(self):
        paths = [Path(f"expenses_2026-03-0{i}.csv") for i in range(1, 4)]
        with patch("pathlib.Path.glob", return_value=paths), patch.object(batch, "fetch_all", return_value=[]), \
                patch.object(batch, "MAX_CHANGED_DATES_PER_RUN", 1), patch.object(batch, "run_day", return_value=True) as run:
            batch.run_available()
            self.assertEqual([c.args[0] for c in run.call_args_list], ["2026-03-03"])

    def test_unchanged_dates_do_not_consume_backfill_budget(self):
        paths = [Path(f"expenses_2026-03-0{i}.csv") for i in range(1, 4)]
        with patch("pathlib.Path.glob", return_value=paths), patch.object(batch, "fetch_all", return_value=[]), \
                patch.object(batch, "MAX_CHANGED_DATES_PER_RUN", 1), \
                patch.object(batch, "run_day", side_effect=["unchanged", True]) as run:
            batch.run_available()
            self.assertEqual([c.args[0] for c in run.call_args_list], ["2026-03-03", "2026-03-02"])

    def test_available_run_recovers_orphaned_status(self):
        class Cursor:
            def execute(self, sql, params=()): self.sql = sql
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def cursor(self):
                class Context:
                    def __enter__(self): self.value = Cursor(); return self.value
                    def __exit__(self, *args): pass
                return Context()
        with patch.object(batch, 'connection', return_value=Conn()), \
             patch('pathlib.Path.glob', return_value=[]), \
             patch.object(batch, 'fetch_all', return_value=[{'batch_id': 1, 'max_event_ts': None}]):
            batch.run_available()
