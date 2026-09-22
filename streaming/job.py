"""First vertical slice: one checkpointed Kafka query archives and serves each batch.

Parquet is written before the database transaction. A retry overwrites only its
uncommitted batch directory. Committed directories are never rewritten. Keep the
checkpoint, raw volume and database together; deleting only one is unsupported.
"""

import json
from datetime import datetime, timedelta, timezone
from psycopg2.extras import Json

from common.db import connection, clock_start
from common.domain import SIM_START, simulated_time, validate_event
from common.archive import build_manifest
from streaming.sink import bulk_serve, EVENT_FIELDS
from common.logging_conf import log
from common.settings import DATA_DIR, KAFKA_BOOTSTRAP, TOPIC, SIM_DAY_SECONDS, EVENT_INTERVAL_SECONDS

# Cached once at startup; the simulation start time is immutable for the
# lifetime of a dataset and does not require a DB round-trip per micro-batch.
_STARTED_AT = None


def event_error(raw):
    try:
        value = json.loads(raw)
        if not isinstance(value, dict):
            return "Event must be a JSON object"
        validate_event(value)
        return None
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        return str(exc)


def process_batch(frame, batch_id):
    # Fence archive writes as well as database writes. Independent checkpoints must
    # never share this dataset; concurrent callbacks are rejected before file I/O.
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(8203, -1)")
        if not cur.fetchone()[0]:
            raise RuntimeError("Another archive writer owns this dataset")
        return _process_batch(frame, batch_id)


def _process_batch(frame, batch_id):
    from pyspark.sql import functions as F

    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pipeline_batches WHERE batch_id = %s", (batch_id,))
        if cur.fetchone():
            return
    # Persist before the first action so unpersist() always runs via finally,
    # even when the batch is empty. Single count() replaces isEmpty() + count().
    cached = frame.persist()
    try:
        count = cached.count()
        if count == 0:
            return
        if count > 2500:
            raise ValueError("Micro-batch exceeds serving safety cap; reduce Kafka offsets per trigger")
        upper = simulated_time(_STARTED_AT, datetime.now(timezone.utc), SIM_DAY_SECONDS) + timedelta(
            seconds=2 * EVENT_INTERVAL_SECONDS * 86400 / SIM_DAY_SECONDS)
        frame = frame.withColumn("error", F.when(F.col("error").isNotNull(), F.col("error")).when(
            (F.try_to_timestamp("event.event_ts") < F.lit(SIM_START)) |
            (F.try_to_timestamp("event.event_ts") > F.lit(upper)), F.lit("outside_simulation_time")))
        valid = frame.filter("error IS NULL").select("event.*").withColumn(
            "event_time", F.to_timestamp("event_ts")
        ).withColumn("dt", F.to_date("event_time")).withColumn(
            "time_of_day_bucket",
            F.when(F.hour("event_time") < 6, "night")
             .when(F.hour("event_time") < 12, "morning")
             .when(F.hour("event_time") < 18, "afternoon").otherwise("evening"),
        )
        good_count = valid.count()
        # Also replace an empty uncommitted batch, so a retry cannot retain stale files.
        valid.write.mode("overwrite").partitionBy("dt").parquet(str(DATA_DIR / "raw" / str(batch_id)))
        if count > good_count:
            frame.filter("error IS NOT NULL").select("raw", "error", "partition", "offset").write.mode(
                "overwrite"
            ).json(str(DATA_DIR / "quarantine" / "stream" / str(batch_id)))

        # Spark normalises, enriches, deduplicates and orders each bounded batch.
        # Keep duplicate payloads until conflict detection; never choose one arbitrarily.
        events = valid.select(*EVENT_FIELDS, "ingest_ts", "time_of_day_bucket").collect()
        manifest = build_manifest(DATA_DIR / "raw" / str(batch_id), good_count)
        latest = valid.agg(F.max("event_time")).first()[0] if good_count else None
        with connection() as conn, conn.cursor() as cur:
            # Serialize serving commits even if an operator starts a second writer.
            cur.execute("SELECT pg_advisory_xact_lock(8203, 0)")
            cur.execute("SELECT 1 FROM pipeline_batches WHERE batch_id=%s", (batch_id,))
            if cur.fetchone():
                return
            conflicts = bulk_serve(cur, [row.asDict() for row in events], batch_id)
            cur.execute("""
                INSERT INTO pipeline_batches (batch_id, rows_in, rows_valid, rows_rejected, max_event_ts, archive_manifest)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (batch_id, count, good_count, count - good_count, latest, Json(manifest)))
        log("streaming", "batch_committed", batch_id=batch_id, rows_in=count,
            rows_valid=good_count, rows_rejected=count - good_count, conflicts=conflicts, max_event_ts=latest)
    finally:
        cached.unpersist()


def main():
    global _STARTED_AT
    from pyspark.sql import SparkSession, functions as F, types as T

    spark = SparkSession.builder.appName("fleet-speed-and-raw").config(
        "spark.sql.session.timeZone", "UTC"
    ).config("spark.sql.shuffle.partitions", "2").getOrCreate()
    _STARTED_AT = clock_start()
    spark.sparkContext.setLogLevel("WARN")
    schema = T.StructType([
        T.StructField(name, kind) for name, kind in [
            ("event_id", T.StringType()), ("trip_id", T.StringType()),
            ("vehicle_id", T.StringType()), ("driver_id", T.StringType()),
            ("lat", T.DoubleType()), ("lon", T.DoubleType()), ("speed_kmph", T.DoubleType()),
            ("status", T.StringType()), ("fare_cents", T.LongType()),
            ("trip_completed", T.BooleanType()), ("zone", T.StringType()),
            ("event_ts", T.StringType()), ("ingest_ts", T.StringType()), ("trace_id", T.StringType()),
        ]
    ])
    source = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option(
        "subscribe", TOPIC
    ).option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 2000).load()
    from streaming.validation import validation_error
    parsed = source.selectExpr("CAST(value AS STRING) AS raw", "partition", "offset").withColumn(
        "event", F.from_json("raw", schema)).withColumn("error", validation_error())
    log("streaming", "started", topic=TOPIC)
    query = parsed.writeStream.foreachBatch(process_batch).option(
        "checkpointLocation", str(DATA_DIR / "checkpoints" / "fleet")
    ).trigger(processingTime="5 seconds").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
