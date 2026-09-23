import json
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from common.db import clock_start
from common.domain import simulated_time
from common.logging_conf import log
from common.settings import EVENT_INTERVAL_SECONDS, KAFKA_BOOTSTRAP, SIM_DAY_SECONDS, TOPIC, VEHICLE_COUNT
from simulators.fixtures import make_event


def send_with_recovery(producer, event, attempts=5):
    """Retry transient broker leadership changes with the same event identity."""
    for attempt in range(1, attempts + 1):
        try:
            producer.send(TOPIC, key=event["vehicle_id"].encode(), value=event).get(timeout=30)
            return
        except KafkaError as exc:
            log("producer", "send_retry", event_id=event["event_id"], attempt=attempt,
                error=type(exc).__name__)
            if attempt == attempts:
                raise
            producer.flush(timeout=30)
            time.sleep(min(attempt, 3))


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, acks="all", retries=10,
                             max_in_flight_requests_per_connection=1,
                             value_serializer=lambda value: json.dumps(value).encode())
    started = clock_start()
    # Resume at the current clock tick; restarts do not replay old trips with new dates.
    last_tick = -1
    log("producer", "started", simulated_day_seconds=SIM_DAY_SECONDS)
    while True:
        now = datetime.now(timezone.utc)
        tick = int((now - started).total_seconds() / EVENT_INTERVAL_SECONDS)
        if tick == last_tick:
            time.sleep(0.1)
            continue
        scheduled = started + timedelta(seconds=tick * EVENT_INTERVAL_SECONDS)
        event_time = simulated_time(started, scheduled, SIM_DAY_SECONDS)
        for number in range(1, VEHICLE_COUNT + 1):
            event = make_event(number, tick, event_time, now)
            send_with_recovery(producer, event)
        last_tick = tick
        log("producer", "events_published", rows=VEHICLE_COUNT, tick=tick, event_ts=event_time)


if __name__ == "__main__":
    main()
