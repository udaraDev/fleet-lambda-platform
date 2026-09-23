"""Recover damaged legacy Parquet batches from their exact Kafka offset ranges.

Run only with archive writers stopped. The tool fails closed unless both adjacent
Spark checkpoint offset logs and every Kafka record are still available. It writes
new MinIO objects first and replaces the manifest in one database transaction.
"""

import argparse
import hashlib
import io
import json
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer, TopicPartition
from psycopg2.extras import Json

from common.archive import _get_fs
from common.db import connection, fetch_all
from common.domain import parse_timestamp, validate_event
from common.settings import DATA_DIR, KAFKA_BOOTSTRAP, MINIO_BUCKET, TOPIC


def _fill_from_identity_ledger(batch_id, rows):
    """Recreate missing deterministic simulator rows and verify their fingerprints."""
    from simulators.fixtures import make_event
    from streaming.sink import fingerprint

    present = {row["event_id"] for row in rows}
    identities = fetch_all("""SELECT event_id,fingerprint,event_ts
        FROM stream_event_keys WHERE batch_id=%s ORDER BY event_ts,event_id""", (batch_id,))
    for identity in identities:
        if identity["event_id"] in present:
            continue
        try:
            vehicle_text, tick_text = identity["event_id"].split("-E-")
            vehicle_number = int(vehicle_text.removeprefix("V-"))
            tick = int(tick_text)
        except (ValueError, AttributeError) as exc:
            raise ValueError(f"Cannot reconstruct non-simulator event {identity['event_id']}") from exc
        event = make_event(vehicle_number, tick, identity["event_ts"], identity["event_ts"])
        if fingerprint(event) != identity["fingerprint"]:
            raise ValueError(f"Identity verification failed for {identity['event_id']}")
        rows.append(event)
    return rows


def _offsets(batch_id):
    path = DATA_DIR / "checkpoints" / "fleet" / "offsets" / str(batch_id)
    if not path.is_file() or not path.stat().st_size:
        raise ValueError(f"Missing legacy checkpoint offset log: {path}")
    return json.loads(path.read_text(encoding="utf-8").splitlines()[-1])[TOPIC]


def recover(batch_id, start_override=None):
    ledger = fetch_all("""SELECT rows_in,rows_valid,rows_rejected
        ,max_event_ts FROM pipeline_batches WHERE batch_id=%s""", (batch_id,))
    if not ledger:
        raise ValueError(f"Unknown committed batch: {batch_id}")
    ledger = ledger[0]
    start = start_override or _offsets(batch_id - 1)
    try:
        end = _offsets(batch_id)
    except ValueError:
        end = None
    partitions = [TopicPartition(TOPIC, int(partition)) for partition in start]
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False,
                             consumer_timeout_ms=5000, value_deserializer=lambda value: json.loads(value))
    consumer.assign(partitions)
    beginnings = consumer.beginning_offsets(partitions)
    rows = []
    if end is not None:
        for tp in partitions:
            first = int(start[str(tp.partition)])
            if beginnings[tp] > first:
                raise ValueError(f"Kafka retention removed batch {batch_id} partition {tp.partition}")
            consumer.seek(tp, first)
        while any(consumer.position(tp) < int(end[str(tp.partition)]) for tp in partitions):
            records = consumer.poll(timeout_ms=1000, max_records=1000)
            if not records:
                raise ValueError(f"Kafka did not return the complete offset range for batch {batch_id}")
            for tp, messages in records.items():
                limit = int(end[str(tp.partition)])
                for message in messages:
                    if message.offset < limit:
                        rows.append(validate_event(message.value))
    else:
        # The final legacy checkpoint logs can themselves be truncated after a
        # hard shutdown. Derive each partition boundary from the committed
        # max_event_ts, then verify the total row count below.
        end = {}
        for tp in partitions:
            first = int(start[str(tp.partition)])
            if beginnings[tp] > first:
                raise ValueError(f"Kafka retention removed batch {batch_id} partition {tp.partition}")
            consumer.assign([tp])
            consumer.seek(tp, first)
            while True:
                records = consumer.poll(timeout_ms=1000, max_records=1).get(tp, [])
                if not records:
                    raise ValueError(f"Cannot derive Kafka boundary for batch {batch_id}")
                message = records[0]
                event = validate_event(message.value)
                if parse_timestamp(event["event_ts"]) > ledger["max_event_ts"]:
                    end[str(tp.partition)] = message.offset
                    break
                rows.append(event)
    consumer.close()
    if len(rows) < ledger["rows_valid"]:
        rows = _fill_from_identity_ledger(batch_id, rows)
    if len(rows) != ledger["rows_in"] or len(rows) != ledger["rows_valid"]:
        raise ValueError(
            f"Recovered row count {len(rows)} does not match committed valid/input counts "
            f"{ledger['rows_valid']}/{ledger['rows_in']}")

    by_date = defaultdict(list)
    for event in rows:
        event = dict(event)
        event_time = parse_timestamp(event["event_ts"])
        event["event_time"] = event_time
        hour = event_time.hour
        event["time_of_day_bucket"] = (
            "night" if hour < 6 else "morning" if hour < 12 else
            "afternoon" if hour < 18 else "evening")
        by_date[event_time.date().isoformat()].append(event)

    fs = _get_fs()
    files = []
    for report_date, events in sorted(by_date.items()):
        relative = f"dt={report_date}/recovered-{batch_id}.parquet"
        data = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(events), data, compression="snappy")
        payload = data.getvalue()
        destination = f"{MINIO_BUCKET}/{batch_id}/{relative}"
        fs.makedirs(destination.rsplit("/", 1)[0], exist_ok=True)
        with fs.open(destination, "wb") as handle:
            handle.write(payload)
        files.append({"path": relative, "sha256": hashlib.sha256(payload).hexdigest(),
                      "rows": len(events), "recovered_from": "kafka_offsets"})
    manifest = {"version": 1, "files": files, "rows": len(rows),
                "recovery": {"source": "kafka_offsets", "batch_id": batch_id}}
    with connection() as conn, conn.cursor() as cur:
        cur.execute("UPDATE pipeline_batches SET archive_manifest=%s WHERE batch_id=%s",
                    (Json(manifest), batch_id))
    print(f"Recovered batch {batch_id}: {len(rows)} rows, {len(files)} object(s)", flush=True)
    return end


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_ids", nargs="+", type=int)
    args = parser.parse_args()
    previous_end = None
    for batch_id in args.batch_ids:
        previous_end = recover(batch_id, previous_end)


if __name__ == "__main__":
    main()
