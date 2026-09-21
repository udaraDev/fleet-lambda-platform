"""Read-only smoke checks against a running stack (standard library only)."""

import argparse
import json
import time
import urllib.error
import urllib.request


def get_json(base, path):
    with urllib.request.urlopen(base + path, timeout=10) as response:
        return json.load(response)


def verify(base, wait_seconds):
    deadline = time.monotonic() + wait_seconds
    last_observation = "No response received"
    while True:
        try:
            health = get_json(base, "/health/pipeline")
            fleet = get_json(base, "/metrics/fleet")
            dates = get_json(base, "/reports/daily")
            last_observation = (f"healthy={health['healthy']}, "
                                f"reporting_vehicles={fleet['reporting_vehicles']}, reports={len(dates)}")
            if health["healthy"] and fleet["reporting_vehicles"] > 0 and dates:
                break
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_observation = f"{type(exc).__name__}: {exc}"
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Verification timed out at {base}. Last observation: {last_observation}")
        time.sleep(5)

    # Oldest day is stable after the source has moved beyond its boundary.
    report = get_json(base, "/reports/daily/" + dates[-1]["dt"])
    assert report["currency"] == "LKR" and report["money_unit"] == "cents"
    identifiers = [row["vehicle_id"] for row in report["vehicles"]]
    assert len(identifiers) == len(set(identifiers)), "Duplicate vehicle/day results"
    for row in report["vehicles"]:
        if row["reconciliation_status"] == "complete":
            expected = row["revenue_cents"] - row["fuel_cents"] - row["maintenance_cents"]
            assert row["profit_cents"] == expected, "Profit equation failed"
            assert row["is_unprofitable"] == (expected < 0)
        else:
            assert row["profit_cents"] is None, "Missing cost incorrectly treated as zero"
    zones = get_json(base, "/metrics/zones")
    assert zones, "No zone metrics"
    print(json.dumps({"result": "passed", "report_date": report["date"],
                      "reported_vehicles": len(identifiers), "zones": len(zones),
                      "checks": ["live ingestion health", "fleet metrics", "zone metrics",
                                 "daily report", "unique vehicle/day", "profit arithmetic"]}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8001")
    parser.add_argument("--wait-seconds", type=int, default=480)
    arguments = parser.parse_args()
    verify(arguments.base_url.rstrip("/"), arguments.wait_seconds)
