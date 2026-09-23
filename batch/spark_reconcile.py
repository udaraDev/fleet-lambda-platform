"""Distributed trip aggregation; only one summary row per vehicle reaches Python."""

from datetime import date, datetime, time, timedelta, timezone
from math import ceil
from common.domain import SIM_START

from common.settings import EVENT_INTERVAL_SECONDS, SIM_DAY_SECONDS

_session = None


def session():
    global _session
    if _session is None:
        from pyspark.sql import SparkSession
        from common.settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        _session = (SparkSession.builder.master("local[2]").appName("fleet-daily-reconciliation")
                    .config("spark.sql.session.timeZone", "UTC")
                    .config("spark.sql.ansi.enabled", "true")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.ui.enabled", "false")
                    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .getOrCreate())
        _session.sparkContext.setLogLevel("WARN")
    return _session


def aggregate(paths, expenses, report_date, known_vehicles):
    if not paths:
        # No input to distribute: return explicit unknowns, never invented zero-profit facts.
        costs = {c["vehicle_id"]: c for c in expenses}
        coverage = {v: {"complete": False, "observations": 0, "conflict": False} for v in known_vehicles}
        return [{"dt": report_date, "vehicle_id": v, "trips": 0, "revenue_cents": 0,
                 "fuel_cents": costs.get(v, {}).get("fuel_cents"),
                 "maintenance_cents": costs.get(v, {}).get("maintenance_cents"),
                 "profit_cents": None, "margin_pct": None, "is_unprofitable": None,
                 "reconciliation_status": "incomplete_telemetry"} for v in sorted(known_vehicles)], coverage, {}
    from pyspark.sql import functions as F, Window

    spark = session()
    start = datetime.combine(date.fromisoformat(report_date), time.min, timezone.utc)
    end = start + timedelta(days=1)
    step = EVENT_INTERVAL_SECONDS * 86400 / SIM_DAY_SECONDS
    expected_observations = ceil((end - SIM_START).total_seconds() / step) - ceil((start - SIM_START).total_seconds() / step)
    expected = spark.createDataFrame([(v,) for v in sorted(known_vehicles)], "vehicle_id string")
    cost = spark.createDataFrame(expenses, "vehicle_id string, fuel_cents long, maintenance_cents long")
    coverage = {}
    conflicted = set()
    if paths:
        raw = spark.read.parquet(*paths)
        # New archives already store the typed timestamp; parse only legacy fixtures.
        if "event_time" not in raw.columns:
            raw = raw.withColumn("event_time", F.try_to_timestamp("event_ts"))
        if raw.filter(F.col("event_time").isNull() | (F.col("event_time") < F.lit(start)) |
                      (F.col("event_time") >= F.lit(end))).limit(1).count():
            raise ValueError("Archive timestamp does not match its date partition")
        if raw.join(expected, "vehicle_id", "left_anti").limit(1).count():
            raise ValueError("Unknown vehicle in committed archive")
        # Ingest timestamps may differ on a retry; business event content may not.
        from common.events import EVENT_FIELDS
        # Match live business identity: fixed schema, UTC instant, not timestamp spelling.
        fields = [F.col("event_time").alias("event_ts") if c == "event_ts" else F.col(c)
                  for c in EVENT_FIELDS]
        signatures = raw.withColumn("signature", F.to_json(F.struct(*fields)))
        bad_ids = signatures.groupBy("event_id").agg(F.countDistinct("signature").alias("n")).filter("n > 1")
        event_conflicts = raw.join(bad_ids, "event_id").select("vehicle_id").distinct()
        clean = raw.join(bad_ids.select("event_id"), "event_id", "left_anti").dropDuplicates(["event_id"])
        completed = clean.filter(F.col("trip_completed"))
        bad_trips = completed.groupBy("trip_id").agg(F.countDistinct(F.struct(
            "vehicle_id", "fare_cents", "event_time", "zone")).alias("n")).filter("n > 1")
        conflicts = event_conflicts.union(completed.join(bad_trips, "trip_id").select("vehicle_id")).distinct()
        conflicted = {r.vehicle_id for r in conflicts.collect()}
        trips = completed.join(bad_trips.select("trip_id"), "trip_id", "left_anti").dropDuplicates(["trip_id"])
        totals = trips.groupBy("vehicle_id").agg(F.count("*").alias("trips"), F.sum("fare_cents").alias("revenue_cents"))
        ordered = clean.select("vehicle_id", "event_time", "zone").distinct().withColumn(
            "previous", F.lag("event_time").over(Window.partitionBy("vehicle_id").orderBy("event_time")))
        summary = ordered.groupBy("vehicle_id").agg(F.min("event_time").alias("first"),
            F.max("event_time").alias("last"), F.count("*").alias("observations"),
            F.max(F.col("event_time").cast("double") - F.col("previous").cast("double")).alias("max_gap"),
            F.last("zone", ignorenulls=True).alias("zone"))
        for row in summary.collect():
            complete = ((row.first.replace(tzinfo=timezone.utc) - start).total_seconds() <= step + 0.001
                        and (end - row.last.replace(tzinfo=timezone.utc)).total_seconds() <= step + 0.001
                        and (row.max_gap or 0) <= step * 1.5 and row.observations >= expected_observations)
            coverage[row.vehicle_id] = {"complete": complete, "observations": row.observations,
                                       "conflict": row.vehicle_id in conflicted,
                                       "zone": getattr(row, "zone", "UNKNOWN")}
    else:
        totals = spark.createDataFrame([], "vehicle_id string, trips long, revenue_cents long")
    for vehicle in known_vehicles:
        coverage.setdefault(vehicle, {"complete": False, "observations": 0, "conflict": vehicle in conflicted})
    coverage_df = spark.createDataFrame([(v, c["complete"], c["conflict"]) for v, c in coverage.items()],
                                       "vehicle_id string, telemetry_complete boolean, conflict boolean")
    result = (expected.join(totals, "vehicle_id", "left").join(cost, "vehicle_id", "left")
              .join(coverage_df, "vehicle_id").fillna({"trips": 0, "revenue_cents": 0})
              .withColumn("reconciliation_status", F.when(F.col("conflict"), "conflicting_events")
                          .when(~F.col("telemetry_complete"), "incomplete_telemetry")
                          .when(F.col("fuel_cents").isNull(), "missing_expenses").otherwise("complete"))
              .withColumn("profit_cents", F.when(F.col("reconciliation_status") == "complete",
                          F.col("revenue_cents") - F.col("fuel_cents") - F.col("maintenance_cents")))
              .withColumn("margin_pct", F.when(F.col("revenue_cents") > 0,
                          F.round(F.col("profit_cents") * 100.0 / F.col("revenue_cents"), 2)))
              .withColumn("is_unprofitable", F.col("profit_cents") < 0)
              .withColumn("dt", F.lit(report_date)))
    # Financial zone totals come from conflict-free completed trips. ``ordered``
    # intentionally contains only telemetry coverage columns.
    zone_summary = trips.groupBy("zone").agg(
        F.count("*").alias("trips"),
        F.sum("fare_cents").alias("revenue_cents"),
        F.approx_count_distinct("vehicle_id").alias("vehicles")
    )
    zone_agg = {r.zone: {"trips": r.trips, "revenue_cents": r.revenue_cents, "vehicles": r.vehicles} for r in zone_summary.collect()}

    columns = ["dt", "vehicle_id", "trips", "revenue_cents", "fuel_cents", "maintenance_cents",
               "profit_cents", "margin_pct", "is_unprofitable", "reconciliation_status"]
    return [r.asDict() for r in result.select(*columns).orderBy("vehicle_id").collect()], coverage, zone_agg
