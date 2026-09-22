import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from batch.reconcile import read_events
except ImportError as exc:
    raise unittest.SkipTest("Archive tests need pyarrow and psycopg2") from exc

from simulators.fixtures import sample_events


class ArchiveTests(unittest.TestCase):
    @unittest.skip("Requires MinIO")
    def test_only_committed_batches_and_requested_date_are_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for batch_id, day in ((1, "2026-03-01"), (2, "2026-03-01"), (1, "2026-03-02")):
                partition = root / "raw" / str(batch_id) / f"dt={day}"
                partition.mkdir(parents=True)
                pq.write_table(pa.Table.from_pylist(sample_events()), partition / "part.parquet")
            with patch("batch.reconcile.DATA_DIR", root):
                events = list(read_events("2026-03-01", [1]))
            self.assertEqual(events, sample_events())
