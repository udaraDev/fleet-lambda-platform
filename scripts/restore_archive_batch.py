"""Restore one corrupted immutable archive batch from retained Kafka events."""

import argparse
import hashlib
import io
import json
import time
import uuid
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import Json

from common.archive import _get_fs, build_manifest, digest, manifest_fingerprint
from common.db import connection, fetch_all
from common.domain import SIM_START, parse_timestamp, validate_event
from common.settings import DATA_DIR, KAFKA_BOOTSTRAP, MINIO_BUCKET, TOPIC


def source_ranges(batch):
    """Return the exact raw Kafka ranges needed to rebuild one archive batch."""
    offsets = batch.get("source_offsets") or {}
    if offsets.get("version") != 1 or offsets.get("topic") != TOPIC or not offsets.get("partitions"):
        raise ValueError("Batch has no restorable Kafka source-offset manifest")
    if offsets.get("recovery"):
        raise ValueError("Recovery batches require an exact retained archive copy")
    ranges = {int(partition): {"start": int(item["start"]), "end": int(item["end"]),
                               "rows": int(item["rows"])}
              for partition, item in offsets["partitions"].items()}
    if sum(item["rows"] for item in ranges.values()) != batch["rows_in"]:
        raise ValueError("Kafka source-offset row count does not match the committed batch")
    return ranges


def restore(batch_id):
    from kafka import KafkaConsumer, TopicPartition

    batch = fetch_all("""SELECT batch_id,rows_in,rows_valid,max_event_ts,
        archive_manifest,source_offsets
        FROM pipeline_batches WHERE batch_id=%s""", (batch_id,))
    if not batch:
        raise ValueError(f"Unknown archive batch {batch_id}")
    batch = batch[0]
    # Prefer an exact legacy copy when one exists. This restores the original
    # committed bytes and therefore does not rewrite immutable history.
    manifest = batch.get("archive_manifest") or {}
    legacy_root = DATA_DIR / "raw" / str(batch_id)
    legacy_files = [(item, legacy_root / item["path"])
                    for item in manifest.get("files", [])]
    exact_legacy = bool(legacy_files) and all(
        path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]
        for item, path in legacy_files)
    if exact_legacy:
        fs = _get_fs()
        token = uuid.uuid4().hex
        root = f"{MINIO_BUCKET}/{batch_id}"
        backup = f"{MINIO_BUCKET}/quarantine/corrupt-batches/{batch_id}-{token}"
        moved_old = False
        try:
            if fs.exists(root):
                fs.mv(root, backup, recursive=True)
                moved_old = True
            for item, source in legacy_files:
                destination = f"{root}/{item['path']}"
                fs.makedirs(destination.rsplit("/", 1)[0], exist_ok=True)
                with source.open("rb") as src, fs.open(destination, "wb") as dst:
                    for block in iter(lambda: src.read(1024 * 1024), b""):
                        dst.write(block)
                if digest(destination, fs) != item["sha256"]:
                    raise ValueError(f"Restored checksum mismatch: {destination}")
            with connection() as conn, conn.cursor() as cur:
                cur.execute("""INSERT INTO dq_archive_verification(batch_id,manifest_sha256)
                    VALUES (%s,%s) ON CONFLICT(batch_id) DO UPDATE SET
                    manifest_sha256=EXCLUDED.manifest_sha256,verified_at=now()""",
                    (batch_id, manifest_fingerprint(manifest)))
        except Exception:
            if fs.exists(root):
                fs.rm(root, recursive=True)
            if moved_old and fs.exists(backup):
                fs.mv(backup, root, recursive=True)
            raise
        print(json.dumps({"batch_id": batch_id, "rows_restored": batch["rows_valid"],
                          "source": "exact_legacy_copy", "corrupt_backup": backup}, indent=2))
        return

    ranges = source_ranges(batch)

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    partitions = [TopicPartition(TOPIC, p) for p in sorted(ranges)]
    consumer.assign(partitions)
    beginnings = consumer.beginning_offsets(partitions)
    ends = consumer.end_offsets(partitions)
    for tp in partitions:
        requested = ranges[tp.partition]
        if beginnings[tp] > requested["start"] or ends[tp] < requested["end"]:
            raise ValueError(f"Kafka retention does not contain partition {tp.partition} "
                             f"offsets [{requested['start']},{requested['end']})")
        consumer.seek(tp, requested["start"])
    recovered = []
    empty_deadline = time.monotonic() + 30
    try:
        while any(consumer.position(tp) < ranges[tp.partition]["end"] for tp in partitions):
            polled = consumer.poll(timeout_ms=1000, max_records=5000)
            if not polled:
                if time.monotonic() >= empty_deadline:
                    raise ValueError("Kafka did not return the complete retained offset snapshot")
                continue
            empty_deadline = time.monotonic() + 30
            for tp, messages in polled.items():
                for message in messages:
                    if message.offset >= ranges[tp.partition]["end"]:
                        continue
                    try:
                        event = validate_event(message.value)
                        event_time = parse_timestamp(event["event_ts"])
                    except (TypeError, ValueError, KeyError):
                        continue
                    if SIM_START <= event_time <= batch["max_event_ts"]:
                        recovered.append(event)
    finally:
        consumer.close()
    if len(recovered) != batch["rows_valid"]:
        raise ValueError("Reconstructed valid-row count does not match the committed archive")

    by_date = defaultdict(list)
    for event in recovered:
        item = dict(event)
        event_time = parse_timestamp(item["event_ts"])
        item["event_time"] = event_time
        item["time_of_day_bucket"] = (
            "night" if event_time.hour < 6 else "morning" if event_time.hour < 12
            else "afternoon" if event_time.hour < 18 else "evening")
        by_date[event_time.date().isoformat()].append(item)
    if max(parse_timestamp(e["event_ts"]) for e in recovered) != batch["max_event_ts"]:
        raise ValueError("Recovered maximum event time differs from the committed batch")

    fs = _get_fs()
    token = uuid.uuid4().hex
    temporary = f"{MINIO_BUCKET}/repairs/{batch_id}-{token}"
    root = f"{MINIO_BUCKET}/{batch_id}"
    backup = f"{MINIO_BUCKET}/quarantine/corrupt-batches/{batch_id}-{token}"
    for report_date, events in sorted(by_date.items()):
        data = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(events), data, compression="snappy")
        destination = f"{temporary}/dt={report_date}/restored.parquet"
        fs.makedirs(destination.rsplit("/", 1)[0], exist_ok=True)
        with fs.open(destination, "wb") as handle:
            handle.write(data.getvalue())

    moved_old = False
    try:
        if fs.exists(root):
            fs.mv(root, backup, recursive=True)
            moved_old = True
        fs.mv(temporary, root, recursive=True)
        manifest = build_manifest(batch_id, len(recovered))
        with connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE pipeline_batches SET archive_manifest=%s WHERE batch_id=%s",
                        (Json(manifest), batch_id))
            cur.execute("""INSERT INTO dq_archive_verification(batch_id,manifest_sha256)
                VALUES (%s,%s) ON CONFLICT(batch_id) DO UPDATE SET
                manifest_sha256=EXCLUDED.manifest_sha256,verified_at=now()""",
                (batch_id, manifest_fingerprint(manifest)))
    except Exception:
        if fs.exists(root):
            fs.rm(root, recursive=True)
        if moved_old and fs.exists(backup):
            fs.mv(backup, root, recursive=True)
        raise
    print(json.dumps({"batch_id": batch_id, "rows_restored": len(recovered),
                      "corrupt_backup": backup}, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_id", type=int)
    restore(parser.parse_args().batch_id)


if __name__ == "__main__":
    main()
