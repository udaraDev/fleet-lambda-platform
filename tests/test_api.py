import unittest
from decimal import Decimal
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
    from api.main import app
except ImportError as exc:
    raise unittest.SkipTest("API tests need FastAPI, httpx and psycopg2") from exc


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_liveness_does_not_require_database(self):
        self.assertEqual(self.client.get("/health").json(), {"status": "alive"})

    @patch("api.main.fetch_all")
    def test_pipeline_healthy_with_postgres_decimals(self, fetch):
        fetch.return_value = [{"last_event_age_seconds": Decimal("1.5"),
                               "rows_in": Decimal(120), "rows_rejected": Decimal(0)}]
        response = self.client.get("/health/pipeline")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["healthy"])

    @patch("api.main.fetch_all")
    def test_no_data_and_stale_data_return_503(self, fetch):
        for age in (None, Decimal("121")):
            fetch.return_value = [{"last_event_age_seconds": age, "rows_in": 0, "rows_rejected": 0}]
            with self.subTest(age=age):
                response = self.client.get("/health/pipeline")
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json()["status"], "no_recent_data")

    @patch("api.main.fetch_all", side_effect=RuntimeError("database down"))
    def test_database_failure_returns_503(self, fetch):
        self.assertEqual(self.client.get("/health/pipeline").status_code, 503)

    @patch("api.main.fetch_all", return_value=[])
    def test_missing_report_returns_404(self, fetch):
        self.assertEqual(self.client.get("/reports/daily/2026-03-01").status_code, 404)

    def test_invalid_date_and_threshold_are_rejected(self):
        self.assertEqual(self.client.get("/reports/daily/not-a-date").status_code, 422)
        self.assertEqual(self.client.get("/alerts/active?idle_minutes=-1").status_code, 422)

    @patch("api.main.fetch_all")
    def test_report_preserves_unknown_profit(self, fetch):
        fetch.return_value = [{"vehicles": [{"vehicle_id": "V-001", "profit_cents": None,
                               "reconciliation_status": "missing_expenses"}], "publication": {"run_id": "one-version"}}]
        response = self.client.get("/reports/daily/2026-03-01")
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["vehicles"][0]["profit_cents"])
        self.assertEqual(response.json()["money_unit"], "cents")
        self.assertEqual(fetch.call_count, 1, "Rows and metadata must share one statement snapshot")
        self.assertEqual(response.json()['publication']['run_id'], 'one-version')

    @patch("api.main.fetch_all")
    def test_report_health_detects_failed_batch_with_fresh_report(self, fetch):
        from datetime import date
        fetch.side_effect = [[{"failed_dates": 1, "stalled_dates": 0}],
                             [{"latest_report": date(2026, 3, 1), "pending_exports": 0}],
                             [{"expected_report": date(2026, 3, 1)}], [], [{'n': 0}]]
        response = self.client.get("/health/reports")
        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["healthy"])

    @patch("api.main.fetch_all")
    def test_report_health_accepts_published_current_report(self, fetch):
        from datetime import date
        fetch.side_effect = [[{"failed_dates": 0, "stalled_dates": 0}],
                             [{"latest_report": date(2026, 3, 1), "pending_exports": 0}],
                             [{"expected_report": date(2026, 3, 1)}], [], [{'n': 0}]]
        self.assertEqual(self.client.get("/health/reports").status_code, 200)
