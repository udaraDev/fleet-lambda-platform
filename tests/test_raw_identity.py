import unittest

from streaming.raw_job import continuity_errors, replay_plan, source_identity
from streaming.staged_sink import dead_letter_partition
from scripts.restore_archive_batch import source_ranges


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

    def test_continuity_accepts_exact_next_offsets(self):
        offsets, _ = source_identity([
            {"partition": 0, "start_offset": 13, "end_offset": 15, "rows": 2},
            {"partition": 2, "start_offset": 24, "end_offset": 25, "rows": 1},
        ])
        self.assertEqual(continuity_errors(offsets, {0: 13, 2: 24}), {})

    def test_continuity_rejects_gap_and_overlap(self):
        offsets, _ = source_identity([
            {"partition": 0, "start_offset": 15, "end_offset": 16, "rows": 1},
            {"partition": 2, "start_offset": 22, "end_offset": 24, "rows": 2},
        ])
        self.assertEqual(continuity_errors(offsets, {0: 13, 2: 24}), {
            0: {"expected": 13, "actual": 15},
            2: {"expected": 24, "actual": 22},
        })

    def test_replay_plan_trims_committed_prefix_and_rejects_gap(self):
        offsets, _ = source_identity([
            {"partition": 0, "start_offset": 10, "end_offset": 20, "rows": 10},
            {"partition": 1, "start_offset": 25, "end_offset": 30, "rows": 5},
            {"partition": 2, "start_offset": 40, "end_offset": 45, "rows": 5},
        ])
        thresholds, gaps = replay_plan(offsets, {0: 15, 1: 20, 2: 40})
        self.assertEqual(thresholds, {0: 15})
        self.assertEqual(gaps, {1: {"expected": 20, "actual": 25}})

    def test_restore_uses_raw_source_offset_ranges(self):
        batch = {"rows_in": 3, "source_offsets": {
            "version": 1, "topic": "trip-events", "partitions": {
                "0": {"start": 10, "end": 12, "rows": 2},
                "2": {"start": 20, "end": 21, "rows": 1},
            }}}
        self.assertEqual(source_ranges(batch)[2]["start"], 20)

    def test_restore_rejects_non_exact_recovery_snapshot(self):
        with self.assertRaisesRegex(ValueError, "exact retained archive copy"):
            source_ranges({"rows_in": 1, "source_offsets": {
                "version": 1, "topic": "trip-events", "recovery": "event_time_gap",
                "partitions": {"0": {"start": 0, "end": 1, "rows": 1}},
            }})
