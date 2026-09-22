import hashlib
import tempfile
import unittest
from datetime import date
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

    @patch('api.main.fetch_all')
    def test_report_health_detects_historical_hole(self, fetch):
        from fastapi.testclient import TestClient
        from api.main import app
        fetch.side_effect = [[{'failed_dates': 0, 'stalled_dates': 0}],
            [{'latest_report': date(2026, 3, 3), 'pending_exports': 0}],
            [{'expected_report': date(2026, 3, 3)}], [], [{'n': 1}]]
        response = TestClient(app).get('/health/reports')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['missing_dates'], 1)

    @patch('api.main.export_matches', return_value=False)
    @patch('api.main.fetch_all')
    def test_report_health_detects_corrupt_export(self, fetch, check):
        from fastapi.testclient import TestClient
        from api.main import app
        from common.publication import ALGORITHM_VERSION
        fetch.side_effect = [[{'failed_dates': 0, 'stalled_dates': 0}],
            [{'latest_report': date(2026, 3, 1), 'pending_exports': 0}],
            [{'expected_report': date(2026, 3, 1)}],
            [{'dt': date(2026, 3, 1), 'output_sha256': 'bad', 'algorithm_version': ALGORITHM_VERSION}], [{'n': 0}]]
        response = TestClient(app).get('/health/reports')
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['invalid_exports'], 1)
