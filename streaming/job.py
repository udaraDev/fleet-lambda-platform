"""First vertical slice: one checkpointed Kafka query archives and serves each batch.

Parquet is written before the database transaction. A retry overwrites only its
uncommitted batch directory. Committed directories are never rewritten. Keep the
checkpoint, raw volume and database together; deleting only one is unsupported.
"""

import json

from common.db import connection
from common.domain import validate_event
from common.logging_conf import log
from common.settings import DATA_DIR, KAFKA_BOOTSTRAP, TOPIC


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
    from pyspark.sql import functions as F

    if frame.isEmpty():
        return
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pipeline_batches WHERE batch_id = %s", (batch_id,))
        if cur.fetchone():
            return
    frame.persist()
    try:
        count = frame.count()
        valid = frame.filter("error IS NULL").select("event.*").withColumn(
            "event_time", F.to_timestamp("event_ts")
        ).withColumn("dt", F.to_date("event_time")).withColumn(
            "time_of_day_bucket",
            F.when(F.hour("event_time") < 6, "night")
             .when(F.hour("event_time") < 12, "morning")
             .when(F.hour("event_time") < 18, "afternoon").otherwise("evening"),
        )
        good_count = valid.count()
        if good_count:
            valid.write.mode("overwrite").partitionBy("dt").parquet(str(DATA_DIR / "raw" / str(batch_id)))
        if count > good_count:
            frame.filter("error IS NOT NULL").select("raw", "error", "partition", "offset").write.mode(
                "overwrite"
            ).json(str(DATA_DIR / "quarantine" / "stream" / str(batch_id)))

        # Spark normalises, enriches, deduplicates and orders each bounded batch.
        events = valid.dropDuplicates(["event_id"]).orderBy("event_time", "event_id")
        latest = valid.agg(F.max("event_time")).first()[0] if good_count else None
        with connection() as conn, conn.cursor() as cur:
            for row in events.toLocalIterator():
                item = row.asDict()
                cur.execute("""
                    INSERT INTO rt_vehicle_state
                        (vehicle_id, zone, status, idle_since, event_ts, event_id, trace_id)
                    VALUES (%s, %s, %s, CASE WHEN %s = 'idle' THEN %s::timestamptz END, %s, %s, %s)
                    ON CONFLICT (vehicle_id) DO UPDATE SET
                        zone = EXCLUDED.zone, status = EXCLUDED.status,
                        idle_since = CASE
                            WHEN EXCLUDED.status <> 'idle' THEN NULL
                            WHEN rt_vehicle_state.status = 'idle' THEN rt_vehicle_state.idle_since
                            ELSE EXCLUDED.event_ts END,
                        event_ts = EXCLUDED.event_ts, event_id = EXCLUDED.event_id, trace_id = EXCLUDED.trace_id
                    WHERE (EXCLUDED.event_ts, EXCLUDED.event_id) >
                          (rt_vehicle_state.event_ts, rt_vehicle_state.event_id)
                """, (item["vehicle_id"], item["zone"], item["status"], item["status"], item["event_ts"],
                      item["event_ts"], item["event_id"], item["trace_id"]))
                if item["trip_completed"]:
                    cur.execute("""
                        INSERT INTO completed_trips
                            (trip_id, vehicle_id, zone, fare_cents, completed_at, time_of_day_bucket, trace_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                    """, (item["trip_id"], item["vehicle_id"], item["zone"], item["fare_cents"],
                          item["event_ts"], item["time_of_day_bucket"], item["trace_id"]))
                    cur.execute("SELECT vehicle_id, fare_cents, completed_at FROM completed_trips WHERE trip_id=%s",
                                (item["trip_id"],))
                    existing = cur.fetchone()
                    from common.domain import parse_timestamp
                    if existing != (item["vehicle_id"], item["fare_cents"], parse_timestamp(item["event_ts"])):
                        raise ValueError(f"Conflicting completion for trip {item['trip_id']}")
            cur.execute("""
                INSERT INTO pipeline_batches (batch_id, rows_in, rows_valid, rows_rejected, max_event_ts)
                VALUES (%s,%s,%s,%s,%s)
            """, (batch_id, count, good_count, count - good_count, latest))
        log("streaming", "batch_committed", batch_id=batch_id, rows_in=count,
            rows_valid=good_count, rows_rejected=count - good_count, max_event_ts=latest)
    finally:
        frame.unpersist()


def main():
    from pyspark.sql import SparkSession, functions as F, types as T

    spark = SparkSession.builder.appName("fleet-speed-and-raw").config(
        "spark.sql.session.timeZone", "UTC"
    ).config("spark.sql.shuffle.partitions", "2").getOrCreate()
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
    parsed = source.selectExpr("CAST(value AS STRING) AS raw", "partition", "offset").withColumn(
        "error", F.udf(event_error, T.StringType())("raw")
    ).withColumn("event", F.from_json("raw", schema))
    log("streaming", "started", topic=TOPIC)
    query = parsed.writeStream.foreachBatch(process_batch).option(
        "checkpointLocation", str(DATA_DIR / "checkpoints" / "fleet")
    ).trigger(processingTime="5 seconds").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
