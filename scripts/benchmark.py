"""Measure end-to-end latency and throughput on a disposable deployment.

Benchmark events are also written to the immutable raw archive, so the command
requires an explicit acknowledgement that the Compose project and its volumes
will be discarded after the run.
"""

import argparse
import json
import math
import time
import uuid
from datetime import datetime, timezone

from common.db import clock_start, fetch_all
from common.domain import simulated_time
from common.settings import KAFKA_BOOTSTRAP, SIM_DAY_SECONDS, TOPIC, vehicles


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return round(ordered[index], 2)


def run_benchmark(eps, duration_seconds=10, disposable_dataset=False):
    if not disposable_dataset:
        raise ValueError(
            "Benchmark events enter the immutable archive. Run against a disposable "
            "Compose project and pass --disposable-dataset."
        )

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    run_id = uuid.uuid4().hex
    known_vehicles = sorted(vehicles())
    started_at = clock_start()
    total_events = eps * duration_seconds
    interval = 1.0 / eps
    sent_at = {}
    start_monotonic = time.monotonic()

    print(f"Starting benchmark {run_id}: {eps} events/sec for {duration_seconds} seconds")
    for index in range(total_events):
        event_id = f"bench-{run_id}-{index}"
        now = datetime.now(timezone.utc)
        vehicle_id = known_vehicles[index % len(known_vehicles)]
        event_time = simulated_time(started_at, now, SIM_DAY_SECONDS)
        event = {
            "event_id": event_id,
            "vehicle_id": vehicle_id,
            "trip_id": f"bench-trip-{run_id}-{index}",
            "driver_id": f"D-{(index % len(known_vehicles)) + 1:03d}",
            "lat": 6.927,
            "lon": 79.861,
            "speed_kmph": 40.0,
            "status": "on_trip",
            "fare_cents": 100,
            "trip_completed": True,
            "zone": "BENCH-ZONE",
            "event_ts": event_time.isoformat(),
            "ingest_ts": now.isoformat(),
            "trace_id": f"bench-trace-{run_id}-{index}",
        }
        sent_at[event_id] = now
        producer.send(TOPIC, event)

        target = start_monotonic + (index + 1) * interval
        remaining = target - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)

    producer.flush()
    producer.close()
    publish_seconds = time.monotonic() - start_monotonic

    deadline = time.monotonic() + 120
    accepted = []
    while time.monotonic() < deadline:
        accepted = fetch_all(
            "SELECT event_id,accepted_at FROM stream_event_keys WHERE event_id LIKE %s",
            (f"bench-{run_id}-%",),
        )
        if len(accepted) >= total_events:
            break
        time.sleep(0.5)

    latencies_ms = [
        max(0.0, (row["accepted_at"] - sent_at[row["event_id"]]).total_seconds() * 1000)
        for row in accepted
        if row["event_id"] in sent_at
    ]
    elapsed = time.monotonic() - start_monotonic
    result = {
        "run_id": run_id,
        "target_eps": eps,
        "duration_seconds": duration_seconds,
        "sent": total_events,
        "accepted": len(accepted),
        "publish_eps": round(total_events / publish_seconds, 2),
        "end_to_end_eps": round(len(accepted) / elapsed, 2),
        "latency_ms_p50": percentile(latencies_ms, 0.50),
        "latency_ms_p95": percentile(latencies_ms, 0.95),
        "latency_ms_max": round(max(latencies_ms), 2) if latencies_ms else None,
    }
    print(json.dumps(result, indent=2))
    if len(accepted) != total_events:
        raise RuntimeError(f"Only {len(accepted)}/{total_events} events were accepted within 120 seconds")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an isolated Kafka-to-PostgreSQL benchmark")
    parser.add_argument("--eps", type=int, choices=[10, 100, 500], required=True)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--disposable-dataset", action="store_true", required=True)
    args = parser.parse_args()
    run_benchmark(args.eps, args.duration, args.disposable_dataset)
