"""Fault-injection scenarios for integration and load testing.

Each function is a generator that yields event dicts ready to be
published to Kafka.  Import and call from producer_stream.py with
--scenario <name> or run directly for standalone injection.

Usage (from fleet-lambda-platform/):
    python -m simulators.scenarios late        # 20 late events
    python -m simulators.scenarios duplicate   # duplicate event_id storm
    python -m simulators.scenarios bad         # malformed payloads -> dead-letter
    python -m simulators.scenarios idle        # force V-001 idle for 10 minutes
    python -m simulators.scenarios all         # run all scenarios in sequence
"""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone

from common.domain import SIM_START, simulated_time, clock_start
from common.settings import (
    KAFKA_BOOTSTRAP, TOPIC, DEAD_LETTER_TOPIC,
    SIM_DAY_SECONDS, EVENT_INTERVAL_SECONDS, VEHICLE_COUNT,
)
from common.logging_conf import log


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_sim():
    """Return the current simulated UTC datetime."""
    return simulated_time(clock_start(), datetime.now(timezone.utc), SIM_DAY_SECONDS)


def _event(vehicle_id, status, fare=0, trip_completed=False,
           event_ts=None, event_id=None, trip_id=None):
    ts = (event_ts or _now_sim()).isoformat()
    return {
        "event_id":      event_id or str(uuid.uuid4()),
        "trip_id":       trip_id  or f"T-{uuid.uuid4().hex[:8]}",
        "driver_id":     f"D-{vehicle_id}",
        "vehicle_id":    vehicle_id,
        "lat":           6.9271,
        "lon":           79.8612,
        "speed_kmph":    0.0 if status == "idle" else 42.0,
        "status":        status,
        "fare_cents":    fare,
        "trip_completed": trip_completed,
        "zone":          "COLOMBO-01",
        "event_ts":      ts,
        "ingest_ts":     datetime.now(timezone.utc).isoformat(),
        "trace_id":      str(uuid.uuid4()),
    }


# ---------------------------------------------------------------------------
# Scenario: late events
# ---------------------------------------------------------------------------

def late_events(count=20, lag_sim_days=0.5):
    """Yield events with timestamps lag_sim_days behind the current sim clock.

    These should be accepted by the batch restatement path but may be
    outside the streaming watermark window.
    """
    lag = timedelta(seconds=lag_sim_days * SIM_DAY_SECONDS)
    for i in range(count):
        vehicle = f"V-{(i % VEHICLE_COUNT) + 1:03d}"
        ts = _now_sim() - lag
        ev = _event(vehicle, "enroute", fare=5000, event_ts=ts)
        log("scenario", "late_event", vehicle_id=vehicle, lag_sim_days=lag_sim_days, seq=i)
        yield ev


# ---------------------------------------------------------------------------
# Scenario: duplicate event_ids
# ---------------------------------------------------------------------------

def duplicate_events(count=10):
    """Yield the same event_id twice with identical payloads.

    The deduplication gate in sink.bulk_serve should accept the first
    occurrence and silently discard the second (idempotent replay).
    """
    for i in range(count):
        vehicle = f"V-{(i % VEHICLE_COUNT) + 1:03d}"
        shared_id = str(uuid.uuid4())
        shared_trip = f"T-{uuid.uuid4().hex[:8]}"
        ev = _event(vehicle, "on_trip", fare=7500, event_id=shared_id, trip_id=shared_trip)
        log("scenario", "duplicate_event", event_id=shared_id, seq=i)
        yield ev
        yield dict(ev)  # exact duplicate — must be deduplicated


# ---------------------------------------------------------------------------
# Scenario: conflicting events (same trip_id, different fare)
# ---------------------------------------------------------------------------

def conflicting_events(count=5):
    """Yield pairs of events sharing a trip_id but with different fares.

    These should be quarantined in stream_conflicts; the trip must not
    appear in completed_trips with either value.
    """
    for i in range(count):
        vehicle = f"V-{(i % VEHICLE_COUNT) + 1:03d}"
        trip_id = f"T-{uuid.uuid4().hex[:8]}"
        ev_a = _event(vehicle, "on_trip", fare=6000, trip_completed=True, trip_id=trip_id)
        ev_b = _event(vehicle, "on_trip", fare=9999, trip_completed=True, trip_id=trip_id)
        log("scenario", "conflicting_event", trip_id=trip_id, seq=i)
        yield ev_a
        yield ev_b


# ---------------------------------------------------------------------------
# Scenario: malformed payloads -> dead-letter
# ---------------------------------------------------------------------------

def bad_events(count=10):
    """Yield deliberately malformed JSON blobs.

    These must be rejected by the schema validator in streaming/job.py,
    published to the dead-letter topic, and NOT appear in any serving table.
    """
    bad_payloads = [
        b"not json at all",
        json.dumps({"event_id": "MISSING-FIELDS"}).encode(),
        json.dumps({"event_id": str(uuid.uuid4()), "vehicle_id": "UNKNOWN-99",
                    "status": "flying", "event_ts": "not-a-timestamp"}).encode(),
        b"",
        json.dumps({"event_id": str(uuid.uuid4()), "vehicle_id": "V-001",
                    "status": "idle", "event_ts": "2020-01-01T00:00:00Z",
                    "fare_cents": -999}).encode(),  # outside simulation window
    ]
    for i in range(count):
        payload = bad_payloads[i % len(bad_payloads)]
        log("scenario", "bad_event_injected", seq=i, size=len(payload))
        # Return raw bytes — the producer publishes these directly without
        # JSON serialisation so the schema validator receives garbage.
        yield {"_raw_bytes": payload, "_scenario": "bad"}


# ---------------------------------------------------------------------------
# Scenario: force a vehicle idle for a sustained period
# ---------------------------------------------------------------------------

def force_idle(vehicle_id="V-001", idle_events=30):
    """Emit a stream of idle events for one vehicle.

    After idle_alert_minutes (default 5 sim-minutes), the /alerts/active
    endpoint must return this vehicle.  Use this scenario to verify that
    the idle alert threshold fires correctly.
    """
    for i in range(idle_events):
        ev = _event(vehicle_id, "idle", fare=0)
        log("scenario", "forced_idle_event", vehicle_id=vehicle_id, seq=i)
        yield ev
        time.sleep(EVENT_INTERVAL_SECONDS)


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

def _publish(producer, events, delay=0.1):
    """Publish a list of event dicts to Kafka, skipping raw-bytes scenarios."""
    for ev in events:
        if "_raw_bytes" in ev:
            # Send malformed payload directly
            producer.send(TOPIC, value=ev["_raw_bytes"])
        else:
            producer.send(TOPIC, value=json.dumps(ev).encode(),
                          key=ev["vehicle_id"].encode())
        time.sleep(delay)
    producer.flush()


if __name__ == "__main__":
    import sys
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("kafka-python not installed; run inside the Docker environment.")
        sys.exit(1)

    scenario = sys.argv[1] if len(sys.argv) > 1 else "all"
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    if scenario in ("late", "all"):
        print("Injecting late events...")
        _publish(producer, list(late_events()))

    if scenario in ("duplicate", "all"):
        print("Injecting duplicate events...")
        _publish(producer, list(duplicate_events()))

    if scenario in ("conflict", "all"):
        print("Injecting conflicting events...")
        _publish(producer, list(conflicting_events()))

    if scenario in ("bad", "all"):
        print("Injecting bad events (dead-letter)...")
        _publish(producer, list(bad_events()))

    if scenario in ("idle", "all"):
        print("Forcing V-001 idle...")
        _publish(producer, list(force_idle("V-001", idle_events=20)), delay=0.5)

    producer.close()
    print("Done.")
