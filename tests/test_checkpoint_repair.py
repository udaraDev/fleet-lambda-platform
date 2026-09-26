import json
import tempfile
import unittest
from pathlib import Path

from scripts.repair_spark_checkpoints import invalid_suffix, repair, valid_metadata_log


def write_valid(path: Path):
    path.write_text('v1\n' + json.dumps({"batchWatermarkMs": 0}) + '\n', encoding='utf-8')


class CheckpointRepairTests(unittest.TestCase):
    def test_validates_spark_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / '1'
            write_valid(path)
            self.assertTrue(valid_metadata_log(path))
            path.write_text('', encoding='utf-8')
            self.assertFalse(valid_metadata_log(path))

    def test_returns_only_contiguous_invalid_suffix(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp)
            write_valid(log / '1')
            (log / '2').write_text('', encoding='utf-8')
            (log / '3').write_text('v1\n{', encoding='utf-8')
            self.assertEqual([path.name for path in invalid_suffix(log)], ['2', '3'])

    def test_refuses_internal_corruption(self):
        with tempfile.TemporaryDirectory() as temp:
            log = Path(temp)
            (log / '1').write_text('', encoding='utf-8')
            write_valid(log / '2')
            with self.assertRaises(RuntimeError):
                invalid_suffix(log)

    def test_apply_moves_records_and_checksums_to_backup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'checkpoints'
            offsets = root / 'raw' / 'offsets'
            offsets.mkdir(parents=True)
            write_valid(offsets / '1')
            (offsets / '2').write_text('', encoding='utf-8')
            (offsets / '.2.crc').write_text('crc', encoding='utf-8')
            result = repair(root, apply=True)
            self.assertFalse((offsets / '2').exists())
            backup = Path(result['backup'])
            self.assertTrue((backup / 'raw' / 'offsets' / '2').exists())
            self.assertTrue((backup / 'raw' / 'offsets' / '.2.crc').exists())


if __name__ == '__main__':
    unittest.main()
