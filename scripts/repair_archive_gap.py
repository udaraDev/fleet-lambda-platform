"""Recover a live/raw event-time gap from retained Kafka records.

Run with ``streaming-raw`` stopped. The command snapshots Kafka end offsets,
validates every selected event, verifies accepted identities against PostgreSQL,
writes date-partitioned Parquet to a new immutable batch, and commits one ledger
row only after every object has been checksummed.
"""

import argparse
import hashlib
import io
import json
import time
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition
from psycopg2.extras import Json

from common.archive import _get_fs, build_manifest, manifest_fingerprint
from common.db import connection, fetch_all
from common.domain import parse_timestamp, validate_event
from common.settings import KAFKA_BOOTSTRAP, MINIO_BUCKET, TOPIC
from streaming.sink import fingerprint


def _verify_identities(rows):
    by_id = defaultdict(set)
    for row in rows:
        by_id[row["event_id"]].add(fingerprint(row))
    ids = list(by_id)
    known = {}
    conflicted = set()
    for start in range(0, len(ids), 5000):
        page = ids[start:start + 5000]
        known.update({r["event_id"]: r["fingerprint"] for r in fetch_all(
            "SELECT event_id,fingerprint FROM stream_event_keys WHERE event_id=ANY(%s)",
            (page,))})
        conflicted.update(r["event_id"] for r in fetch_all(
            "SELECT DISTINCT event_id FROM stream_conflicts WHERE event_id=ANY(%s)",
            (page,)))
    for event_id, signatures in by_id.items():
        if event_id in known and signatures != {known[event_id]}:
            raise ValueError(f"Kafka identity mismatch for {event_id}")
        if event_id not in known and event_id not in conflicted:
            raise ValueError(f"Kafka event {event_id} is absent from serving identity/conflict ledgers")


def repair(include_boundary=False):
    with connection() as lock_conn, lock_conn.cursor() as lock_cur:
        lock_cur.execute("SELECT pg_try_advisory_lock(8203,-1)")
        if not lock_cur.fetchone()[0]:
            raise RuntimeError("Stop streaming-raw before repairing the archive gap")

        state = fetch_all("""SELECT max(max_event_ts) AS raw_max,
            (SELECT max(event_ts) FROM stream_event_keys) AS live_max,
            COALESCE(max(batch_id),-1)+1 AS next_batch_id FROM pipeline_batches""")[0]
        if not state["raw_max"] or not state["live_max"]:
            raise ValueError("Both raw and serving event timestamps are required")
        if state["raw_max"] >= state["live_max"]:
            print("Raw archive is already current; nothing to repair.", flush=True)
            return None

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        partitions = [TopicPartition(TOPIC, p) for p in consumer.partitions_for_topic(TOPIC)]
        consumer.assign(partitions)
        starts = consumer.beginning_offsets(partitions)
        ends = consumer.end_offsets(partitions)
        for tp in partitions:
            consumer.seek(tp, starts[tp])

        recovered = []
        try:
            empty_deadline = time.monotonic() + 30
            while any(consumer.position(tp) < ends[tp] for tp in partitions):
                polled = consumer.poll(timeout_ms=1000, max_records=5000)
                if not polled:
                    if time.monotonic() >= empty_deadline:
                        raise ValueError("Kafka did not return the complete retained offset snapshot")
                    continue
                empty_deadline = time.monotonic() + 30
                for tp, messages in polled.items():
                    limit = ends[tp]
                    for message in messages:
                        if message.offset >= limit:
                            continue
                        try:
                            event = validate_event(message.value)
                        except (TypeError, ValueError, KeyError):
                            continue
                        event_time = parse_timestamp(event["event_ts"])
                        boundary_ok = event_time >= state["raw_max"] if include_boundary else event_time > state["raw_max"]
                        if boundary_ok and event_time <= state["live_max"]:
                            recovered.append(event)
        finally:
            consumer.close()

        if not recovered:
            raise ValueError("Kafka retention no longer contains the missing raw interval")
        _verify_identities(recovered)

        batch_id = state["next_batch_id"]
        by_date = defaultdict(list)
        for event in recovered:
            item = dict(event)
            event_time = parse_timestamp(item["event_ts"])
            item["event_time"] = event_time
            item["time_of_day_bucket"] = (
                "night" if event_time.hour < 6 else
                "morning" if event_time.hour < 12 else
                "afternoon" if event_time.hour < 18 else "evening")
            by_date[event_time.date().isoformat()].append(item)

        fs = _get_fs()
        root = f"{MINIO_BUCKET}/{batch_id}"
        for report_date, events in sorted(by_date.items()):
            data = io.BytesIO()
            pq.write_table(pa.Table.from_pylist(events), data, compression="snappy")
            destination = f"{root}/dt={report_date}/recovered-gap.parquet"
            fs.makedirs(destination.rsplit("/", 1)[0], exist_ok=True)
            with fs.open(destination, "wb") as handle:
                handle.write(data.getvalue())

        manifest = build_manifest(batch_id, len(recovered))
        source_offsets = {
            "version": 1, "topic": TOPIC, "recovery": "event_time_gap",
            "partitions": {str(tp.partition): {
                "start": starts[tp], "end": ends[tp]
            } for tp in sorted(partitions, key=lambda item: item.partition)},
            "raw_after": state["raw_max"].isoformat(),
            "live_through": state["live_max"].isoformat(),
        }
        source_fingerprint = hashlib.sha256(json.dumps(
            source_offsets, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        with connection() as conn, conn.cursor() as cur:
            cur.execute("""INSERT INTO pipeline_batches
                (batch_id,source_batch_id,source_offsets,source_fingerprint,
                 rows_in,rows_valid,rows_rejected,max_event_ts,archive_manifest)
                VALUES (%s,NULL,%s,%s,%s,%s,0,%s,%s)""",
                (batch_id, Json(source_offsets), source_fingerprint,
                 len(recovered), len(recovered), max(parse_timestamp(
                     event["event_ts"]) for event in recovered), Json(manifest)))
            cur.execute("""INSERT INTO dq_archive_verification
                (batch_id,manifest_sha256) VALUES (%s,%s)
                ON CONFLICT(batch_id) DO UPDATE SET
                manifest_sha256=EXCLUDED.manifest_sha256,verified_at=now()""",
                (batch_id, manifest_fingerprint(manifest)))
        print(json.dumps({
            "batch_id": batch_id,
            "rows_recovered": len(recovered),
            "dates_recovered": len(by_date),
            "first_date": min(by_date),
            "last_date": max(by_date),
        }, indent=2), flush=True)
        return batch_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-boundary", action="store_true",
                        help="include events exactly at the previous raw maximum")
    args = parser.parse_args()
    repair(args.include_boundary)


if __name__ == "__main__":
    main()
