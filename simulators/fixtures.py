from datetime import timedelta

from common.domain import SIM_START

ZONES = ("COLOMBO-01", "COLOMBO-03", "COLOMBO-07")


def make_event(vehicle_number, tick, event_ts, ingest_ts):
    """One completion per six ticks; event/trip IDs survive producer retries."""
    phase = tick % 6
    parked = vehicle_number % 4 == 0
    vehicle = f"V-{vehicle_number:03d}"
    trip = f"{vehicle}-T-{tick // 6:09d}"
    return {
        "event_id": f"{vehicle}-E-{tick:010d}", "trip_id": None if parked else trip,
        "driver_id": f"D-{vehicle_number:03d}", "vehicle_id": vehicle,
        "lat": 6.90 + vehicle_number % 20 * 0.001, "lon": 79.85 + vehicle_number % 20 * 0.001,
        "speed_kmph": 0.0 if parked or phase in (0, 5) else 25.0 + vehicle_number % 15,
        "status": "idle" if parked or phase in (0, 5) else "enroute" if phase == 1 else "on_trip",
        "fare_cents": (30000 + vehicle_number * 2500) if phase == 5 and not parked else 0,
        "trip_completed": phase == 5 and not parked, "zone": ZONES[vehicle_number % len(ZONES)],
        "event_ts": event_ts.isoformat(), "ingest_ts": ingest_ts.isoformat(), "trace_id": trip,
    }


def expense_rows(report_date, vehicle_count):
    for number in range(1, vehicle_count + 1):
        yield {
            "vehicle_id": f"V-{number:03d}", "fuel_cost": "1800.00",
            "maintenance_cost": "12000.00" if number % 4 == 0 else "200.00",
            "distance_covered": 120 + number, "service_flag": str(number % 4 == 0).lower(),
            "report_date": report_date,
        }


def sample_events():
    """Small, reproducible input for tests and a dependency-free local demo."""
    return [make_event(n, tick, SIM_START + timedelta(minutes=tick), SIM_START)
            for tick in range(150) for n in range(1, 4)]
