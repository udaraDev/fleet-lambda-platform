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
    @patch("common.archive.MINIO_BUCKET")
    @patch("common.archive._get_fs")
    def test_only_committed_batches_and_requested_date_are_read(self, mock_get_fs, mock_bucket):
        import fsspec
        mock_get_fs.return_value = fsspec.filesystem("file")
        with tempfile.TemporaryDirectory() as directory:
            # On Windows, fsspec file paths are like C:/temp/..., which works fine.
            root = Path(directory).as_posix()
            mock_bucket.side_effect = None
            mock_bucket.__str__.return_value = root
            for batch_id, day in ((1, "2026-03-01"), (2, "2026-03-01"), (1, "2026-03-02")):
                partition = Path(directory) / str(batch_id) / f"dt={day}"
                partition.mkdir(parents=True)
                pq.write_table(pa.Table.from_pylist(sample_events()), partition / "part.parquet")

            with patch("common.settings.MINIO_BUCKET", root):
                events = list(read_events("2026-03-01", [1]))
            self.assertEqual(events, sample_events())
