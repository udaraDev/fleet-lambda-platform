import unittest
from datetime import datetime, timedelta, timezone

from common.domain import SIM_START, money_cents, reconcile, simulated_time, validate_event, validate_expenses
from simulators.fixtures import expense_rows, make_event, sample_events


class MoneyTests(unittest.TestCase):
    def test_exact_cents(self):
        self.assertEqual(money_cents("420.50"), 42050)

    def test_reject_invalid_amounts(self):
        for value in ("-1", "NaN", "Infinity", "0.001", "abc"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                money_cents(value)


class ClockTests(unittest.TestCase):
    def test_five_minutes_is_one_day(self):
        start = datetime(2026, 9, 21, tzinfo=timezone.utc)
        self.assertEqual(simulated_time(start, start + timedelta(seconds=300)), SIM_START + timedelta(days=1))

    def test_invalid_clock_scale(self):
        with self.assertRaises(ValueError):
            simulated_time(SIM_START, SIM_START, 0)


class ExpenseTests(unittest.TestCase):
    day = "2026-03-01"
    known = {"V-001", "V-002", "V-003"}

    def test_valid_file(self):
        valid, rejected = validate_expenses(list(expense_rows(self.day, 3)), self.day, self.known)
        self.assertEqual(len(valid), 3)
        self.assertFalse(rejected)

    def test_all_duplicate_rows_quarantined(self):
        rows = list(expense_rows(self.day, 3))
        rows.append(dict(rows[0]))
        valid, rejected = validate_expenses(rows, self.day, self.known)
        self.assertEqual({row["vehicle_id"] for row in valid}, {"V-002", "V-003"})
        self.assertEqual(len(rejected), 2)

    def test_bad_cost_vehicle_date_and_distance(self):
        for field, value in (("fuel_cost", "-1"), ("vehicle_id", "V-999"),
                             ("report_date", "2026-03-02"), ("distance_covered", "NaN"),
                             ("service_flag", "yes")):
            row = next(expense_rows(self.day, 1))
            row[field] = value
            with self.subTest(field=field):
                valid, rejected = validate_expenses([row], self.day, self.known)
                self.assertEqual(len(rejected), 1)
                self.assertFalse(valid)


class ReconciliationTests(unittest.TestCase):
    day = "2026-03-01"

    def setUp(self):
        self.costs, _ = validate_expenses(list(expense_rows(self.day, 3)), self.day,
                                         {"V-001", "V-002", "V-003"})

    def test_count_completed_trips_not_telemetry(self):
        result = reconcile(sample_events(), self.costs, self.day)
        self.assertEqual(result[0]["trips"], 25)
        self.assertEqual(result[0]["revenue_cents"], 812500)
        self.assertEqual(result[0]["profit_cents"], 612500)
        self.assertFalse(result[0]["is_unprofitable"])

    def test_duplicate_event_or_trip_does_not_change_result(self):
        events = sample_events()
        duplicate = dict(events[-1], event_id="retry-with-new-id")
        self.assertEqual(reconcile(events + events + [duplicate], self.costs, self.day),
                         reconcile(events, self.costs, self.day))

    def test_missing_cost_is_unknown(self):
        row = reconcile(sample_events(), [], self.day)[0]
        self.assertIsNone(row["profit_cents"])
        self.assertIsNone(row["is_unprofitable"])
        self.assertEqual(row["reconciliation_status"], "missing_expenses")

    def test_expense_only_vehicle_and_zero_revenue_margin(self):
        row = reconcile([], self.costs, self.day)[0]
        self.assertEqual(row["trips"], 0)
        self.assertEqual(row["profit_cents"], -200000)
        self.assertIsNone(row["margin_pct"])

    def test_conflicting_completion_fails(self):
        events = sample_events()
        with self.assertRaisesRegex(ValueError, "Conflicting completion"):
            reconcile(events + [dict(events[-1], fare_cents=1)], self.costs, self.day)

    def test_completion_date_controls_accounting(self):
        event = make_event(1, 5, SIM_START + timedelta(days=1), SIM_START)
        self.assertEqual(reconcile([event], [], self.day), [])
        self.assertEqual(reconcile([event], [], "2026-03-02")[0]["trips"], 1)

    def test_corrected_expense_restates_result(self):
        first = reconcile(sample_events(), self.costs, self.day)
        changed = [dict(row, fuel_cents=row["fuel_cents"] + 1000) for row in self.costs]
        second = reconcile(sample_events(), changed, self.day)
        self.assertEqual(len(first), len(second))
        self.assertEqual(first[0]["profit_cents"] - 1000, second[0]["profit_cents"])


class EventTests(unittest.TestCase):
    def test_fixtures_valid(self):
        for event in sample_events():
            validate_event(event)

    def test_invalid_fields(self):
        for field, value in (("status", "parked"), ("fare_cents", 1.5), ("lat", 100),
                             ("event_ts", "2026-03-01T00:00:00"), ("trip_completed", "false")):
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_event(dict(sample_events()[0], **{field: value}))


if __name__ == "__main__":
    unittest.main()
