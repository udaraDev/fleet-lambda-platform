"""Independent Speed Serving Consumer.

Reads from Kafka, applies formal Spark tumbling windows + watermarks for zone metrics,
and writes live state to PostgreSQL. Maintains a separate checkpoint from the raw archive.
"""

import json
from datetime import datetime, timedelta, timezone
from psycopg2.extras import execute_values

from common.db import connection, clock_start
from common.domain import SIM_START, simulated_time, validate_event
from streaming.sink import bulk_serve, EVENT_FIELDS
from common.logging_conf import log
from common.settings import DATA_DIR, KAFKA_BOOTSTRAP, TOPIC, SIM_DAY_SECONDS, EVENT_INTERVAL_SECONDS

_STARTED_AT = None


def process_serving_batch(frame, batch_id):
    """Upsert live vehicle states and completed trips to PostgreSQL."""
    from pyspark.sql import functions as F

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
            
        invalid = frame.filter("error IS NOT NULL").select("raw", "error", "partition", "offset").collect()
        if invalid:
            from kafka import KafkaProducer
            from common.settings import KAFKA_BOOTSTRAP
            from streaming.sink import dead_letter_send

            producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            with connection() as conn, conn.cursor() as cur:
                for row in invalid:
                    dead_letter_send(producer, row["raw"], row["error"], row["partition"], row["offset"], batch_id, cur)
            producer.close()

        valid = frame.filter("error IS NULL").select("event.*").withColumn(
            "event_time", F.to_timestamp("event_ts")
        ).withColumn(
            "time_of_day_bucket",
            F.when(F.hour("event_time") < 6, "night")
             .when(F.hour("event_time") < 12, "morning")
             .when(F.hour("event_time") < 18, "afternoon").otherwise("evening")
        )
        
        events = valid.select(*EVENT_FIELDS, "ingest_ts", "time_of_day_bucket").collect()
        
        with connection() as conn, conn.cursor() as cur:
            # Serialize serving commits even if an operator starts a second writer.
            cur.execute("SELECT pg_advisory_xact_lock(8203, 0)")
            conflicts = bulk_serve(cur, [row.asDict() for row in events], batch_id)
            
        log("streaming-speed", "serving_committed", batch_id=batch_id, rows_in=count, conflicts=conflicts)
    finally:
        cached.unpersist()


def process_metrics_batch(frame, batch_id):
    """Upsert tumbling window metrics to PostgreSQL."""
    metrics = frame.collect()
    if not metrics:
        return
        
    with connection() as conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO rt_zone_metrics
            (window_start, zone, active_vehicles, idle_ratio, trips, earnings_cents)
            VALUES %s ON CONFLICT (window_start, zone) DO UPDATE SET
            active_vehicles=EXCLUDED.active_vehicles, 
            idle_ratio=EXCLUDED.idle_ratio,
            trips=EXCLUDED.trips, 
            earnings_cents=EXCLUDED.earnings_cents, 
            updated_at=now()
        """, [(r["window_start"], r["zone"], r["active_vehicles"], r["idle_ratio"], 
               r["trips"], r["earnings_cents"]) for r in metrics])
    log("streaming-speed", "metrics_committed", batch_id=batch_id, zones=len(metrics))


def main():
    global _STARTED_AT
    from pyspark.sql import SparkSession, functions as F, types as T
    from streaming.validation import validation_error

    spark = (SparkSession.builder.appName("fleet-speed")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
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
        
    scale = 86400 / SIM_DAY_SECONDS
    margin = 2 * EVENT_INTERVAL_SECONDS * scale
    sim_start_unix = SIM_START.timestamp()
    upper_expr = F.expr(f"timestamp_seconds({sim_start_unix} + (unix_timestamp(current_timestamp()) - {_STARTED_AT.timestamp()}) * {scale} + {margin})")

    valid = parsed.filter("error IS NULL").select("event.*").withColumn(
        "event_time", F.to_timestamp("event_ts")
    ).filter((F.col("event_time") >= F.lit(SIM_START)) & (F.col("event_time") <= upper_expr))
    
    log("streaming-speed", "started", topic=TOPIC)
    
    # Query 1: Live State Serving (Append mode, micro-batches)
    q1 = parsed.writeStream.foreachBatch(process_serving_batch).option(
        "checkpointLocation", str(DATA_DIR / "checkpoints" / "speed_serving")
    ).trigger(processingTime="5 seconds").start()
    
    # Query 2: Tumbling Window Metrics with Watermarks
    # Event identity is global. Including event_time in the key allowed a
    # conflicting retry with a changed timestamp to be counted twice.
    windowed = valid.withWatermark("event_time", "2 minutes").dropDuplicatesWithinWatermark(["event_id"]).groupBy(
        F.window("event_time", "1 minute").alias("window"),
        "zone"
    ).agg(
        F.approx_count_distinct("vehicle_id").alias("reporting"),
        F.approx_count_distinct(F.when(F.col("status") != "idle", F.col("vehicle_id"))).alias("active_vehicles"),
        (F.approx_count_distinct(F.when(F.col("status") == "idle", F.col("vehicle_id"))) /
         F.approx_count_distinct("vehicle_id")).alias("idle_ratio"),
        F.count(F.when(F.col("trip_completed"), F.col("event_id"))).alias("trips"),
        F.sum(F.when(F.col("trip_completed"), F.col("fare_cents")).otherwise(0)).alias("earnings_cents")
    ).select(
        F.col("window.start").alias("window_start"),
        "zone", "active_vehicles", "idle_ratio", "trips", "earnings_cents"
    )
    
    q2 = windowed.writeStream.foreachBatch(process_metrics_batch).outputMode("update").option(
        "checkpointLocation", str(DATA_DIR / "checkpoints" / "speed_metrics")
    ).trigger(processingTime="5 seconds").start()
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
