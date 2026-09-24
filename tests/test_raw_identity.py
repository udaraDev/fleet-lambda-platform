import unittest

from streaming.raw_job import source_identity
from streaming.staged_sink import dead_letter_partition


class RawArchiveIdentityTests(unittest.TestCase):
    def test_identity_uses_offsets_not_spark_batch_id(self):
        partitions = [
            {"partition": 2, "start_offset": 20, "end_offset": 24, "rows": 4},
            {"partition": 0, "start_offset": 10, "end_offset": 13, "rows": 3},
        ]
        offsets, fingerprint = source_identity(partitions)
        self.assertEqual(list(offsets["partitions"]), ["0", "2"])
        self.assertEqual(offsets["partitions"]["0"]["end"], 13)
        self.assertEqual(len(fingerprint), 64)

    def test_identity_is_stable_for_partition_order(self):
        rows = [
            {"partition": 0, "start_offset": 1, "end_offset": 3, "rows": 2},
            {"partition": 1, "start_offset": 5, "end_offset": 6, "rows": 1},
        ]
        self.assertEqual(source_identity(rows)[1], source_identity(list(reversed(rows)))[1])

    def test_empty_dead_letter_partition_does_not_require_kafka(self):
        dead_letter_partition(iter(()), 1)
