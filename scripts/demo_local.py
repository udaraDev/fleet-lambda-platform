"""Exercise the shared business logic without Docker or third-party packages."""

import json

from common.domain import reconcile, validate_expenses
from simulators.fixtures import expense_rows, sample_events


def main():
    day = "2026-03-01"
    expenses, rejected = validate_expenses(list(expense_rows(day, 3)), day, {"V-001", "V-002", "V-003"})
    assert not rejected
    events = sample_events()
    events.append(events[-1].copy())  # Deliberately duplicated completion.
    print(json.dumps({"mode": "local business-logic demo; no Kafka, Spark or Airflow",
                      "date": day, "currency": "LKR", "money_unit": "cents",
                      "vehicles": reconcile(events, expenses, day)}, indent=2))


if __name__ == "__main__":
    main()
