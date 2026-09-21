"""Pure business rules. Currency is stored as integer LKR cents throughout."""

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

UTC = timezone.utc
SIM_START = datetime(2026, 3, 1, tzinfo=UTC)
STATUSES = {"idle", "enroute", "on_trip"}
EXPENSE_FIELDS = (
    "vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered",
    "service_flag", "report_date",
)


def simulated_time(started_at, now, day_seconds=300):
    if day_seconds <= 0:
        raise ValueError("day_seconds must be positive")
    elapsed = max(0, (now - started_at).total_seconds())
    return SIM_START + timedelta(seconds=elapsed * 86400 / day_seconds)


def parse_timestamp(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone")
    return parsed.astimezone(UTC)


def money_cents(value):
    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid currency amount") from exc
    if not amount.is_finite() or amount < 0 or amount * 100 != (amount * 100).to_integral_value():
        raise ValueError("Money must be nonnegative with at most two decimal places")
    return int(amount * 100)


def validate_event(event):
    required = {"event_id", "trip_id", "vehicle_id", "driver_id", "lat", "lon",
                "speed_kmph", "status", "fare_cents", "trip_completed", "zone",
                "event_ts", "ingest_ts", "trace_id"}
    if required - event.keys():
        raise ValueError("Missing event fields: " + ", ".join(sorted(required - event.keys())))
    for field in ("event_id", "vehicle_id", "driver_id", "zone", "trace_id"):
        if not isinstance(event[field], str) or not event[field].strip():
            raise ValueError(f"{field} must be a nonempty string")
    if event["status"] not in STATUSES:
        raise ValueError("Invalid vehicle status")
    if type(event["fare_cents"]) is not int or not 0 <= event["fare_cents"] <= 2**63 - 1:
        raise ValueError("fare_cents must be a nonnegative integer")
    if type(event["trip_completed"]) is not bool:
        raise ValueError("trip_completed must be boolean")
    if event["trip_id"] is not None and not isinstance(event["trip_id"], str):
        raise ValueError("trip_id must be a string or null")
    if event["trip_completed"] and not event["trip_id"]:
        raise ValueError("Completed trips require trip_id")
    for field, lower, upper in (("lat", -90, 90), ("lon", -180, 180), ("speed_kmph", 0, 200)):
        if type(event[field]) not in (int, float) or not lower <= event[field] <= upper:
            raise ValueError(f"Invalid {field}")
    parse_timestamp(event["event_ts"])
    parse_timestamp(event["ingest_ts"])
    return event


def validate_expenses(rows, report_date, known_vehicles):
    """Quarantine every copy of duplicate vehicles, rather than silently picking one."""
    counts = Counter(row.get("vehicle_id") for row in rows)
    valid, rejected = [], []
    for row_number, row in enumerate(rows, start=2):
        try:
            if set(EXPENSE_FIELDS) - row.keys():
                raise ValueError("Missing expense columns")
            vehicle = row["vehicle_id"]
            if vehicle not in known_vehicles:
                raise ValueError("Unknown vehicle_id")
            if counts[vehicle] != 1:
                raise ValueError("Duplicate vehicle_id")
            if row["report_date"] != report_date:
                raise ValueError("Incorrect report_date")
            distance = Decimal(str(row["distance_covered"]))
            if not distance.is_finite() or not 0 <= distance <= 2000:
                raise ValueError("Implausible distance_covered")
            if str(row["service_flag"]).lower() not in {"true", "false", "0", "1"}:
                raise ValueError("Invalid service_flag")
            valid.append({"vehicle_id": vehicle, "fuel_cents": money_cents(row["fuel_cost"]),
                          "maintenance_cents": money_cents(row["maintenance_cost"])})
        except (ValueError, TypeError, InvalidOperation) as exc:
            rejected.append({"row_number": row_number, "reason": str(exc), "row": row})
    return valid, rejected


def reconcile(events, expenses, report_date):
    """Count each completed trip once, assigned to its UTC completion date.

    Missing costs remain unknown. Expense-only vehicles have zero revenue and
    retain their costs. Conflicting completions are rejected instead of guessed.
    """
    trips, totals = {}, {}
    for event in events:
        validate_event(event)
        if not event["trip_completed"] or parse_timestamp(event["event_ts"]).date().isoformat() != report_date:
            continue
        trip = (event["vehicle_id"], event["fare_cents"], parse_timestamp(event["event_ts"]))
        key = event["trip_id"]
        if key in trips and trips[key] != trip:
            raise ValueError(f"Conflicting completion for trip {key}")
        if key not in trips:
            trips[key] = trip
            count, revenue = totals.get(trip[0], (0, 0))
            totals[trip[0]] = (count + 1, revenue + trip[1])
    costs = {row["vehicle_id"]: row for row in expenses}
    output = []
    for vehicle in sorted(totals.keys() | costs.keys()):
        count, revenue = totals.get(vehicle, (0, 0))
        cost = costs.get(vehicle)
        profit = revenue - cost["fuel_cents"] - cost["maintenance_cents"] if cost else None
        output.append({
            "dt": report_date, "vehicle_id": vehicle, "trips": count, "revenue_cents": revenue,
            "fuel_cents": cost["fuel_cents"] if cost else None,
            "maintenance_cents": cost["maintenance_cents"] if cost else None,
            "profit_cents": profit,
            "margin_pct": round(profit * 100 / revenue, 2) if profit is not None and revenue else None,
            "is_unprofitable": profit < 0 if profit is not None else None,
            "reconciliation_status": "complete" if cost else "missing_expenses",
        })
    return output
