"""Independent Raw Archive Consumer.

Reads from Kafka, validates, and writes Parquet directly to MinIO.
Updates pipeline_batches in Postgres to signal completion to Airflow.
Maintains its own checkpoint.
"""

import json
from datetime import datetime, timedelta, timezone
from psycopg2.extras import Json

from common.db import connection, clock_start
from common.domain import SIM_START, simulated_time, validate_event
from common.archive import build_manifest
from common.logging_conf import log
from common.settings import (DATA_DIR, KAFKA_BOOTSTRAP, TOPIC, SIM_DAY_SECONDS, 
                             EVENT_INTERVAL_SECONDS, MINIO_ENDPOINT, MINIO_ACCESS_KEY, 
                             MINIO_SECRET_KEY, MINIO_BUCKET)

_STARTED_AT = None

def process_batch(frame, batch_id):
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

    cached = frame.persist()
    try:
        count = cached.count()
        if count == 0:
            return
        
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
             .when(F.hour("event_time") < 18, "afternoon").otherwise("evening")
        )
        
        good_count = valid.count()
        
        # Write Parquet directly to MinIO (S3)
        s3_path = f"s3a://{MINIO_BUCKET}/{batch_id}"
        valid.write.mode("overwrite").partitionBy("dt").parquet(s3_path)
        
        if count > good_count:
            frame.filter("error IS NOT NULL").select("raw", "error", "partition", "offset").write.mode(
                "overwrite"
            ).json(f"s3a://{MINIO_BUCKET}/quarantine/{batch_id}")

        manifest = build_manifest(batch_id, good_count)
        latest = valid.agg(F.max("event_time")).first()[0] if good_count else None
        
        with connection() as conn, conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_batches (batch_id, rows_in, rows_valid, rows_rejected, max_event_ts, archive_manifest)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (batch_id, count, good_count, count - good_count, latest, Json(manifest)))
            
        log("streaming-raw", "batch_committed", batch_id=batch_id, rows_in=count,
            rows_valid=good_count, rows_rejected=count - good_count, max_event_ts=latest)
    finally:
        cached.unpersist()


def main():
    global _STARTED_AT
    from pyspark.sql import SparkSession, functions as F, types as T
    from streaming.validation import validation_error

    spark = (SparkSession.builder.appName("fleet-raw")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
             .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .getOrCreate())
             
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
    
    parsed = source.selectExpr("CAST(value AS STRING) AS raw", "partition", "offset").withColumn(
        "event", F.from_json("raw", schema)).withColumn("error", validation_error())
        
    log("streaming-raw", "started", topic=TOPIC)
    
    query = parsed.writeStream.foreachBatch(process_batch).option(
        "checkpointLocation", str(DATA_DIR / "checkpoints" / "raw")
    ).trigger(processingTime="5 seconds").start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
