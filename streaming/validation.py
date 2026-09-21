"""Native Spark expressions; invalid events remain available to quarantine."""

from common.settings import vehicles


def validation_error():
    from pyspark.sql import functions as F

    e = lambda name: F.col("event." + name)
    required_text = ["event_id", "driver_id", "vehicle_id", "zone", "trace_id"]
    error = F.when(F.col("event").isNull(), F.lit("invalid_json"))
    for name in ("trip_id", "event_id", "driver_id", "vehicle_id", "lat", "lon", "speed_kmph",
                 "status", "fare_cents", "trip_completed", "zone", "event_ts", "ingest_ts", "trace_id"):
        error = error.when(~F.array_contains(F.json_object_keys("raw"), name), F.lit("missing_" + name))
    for name in required_text:
        error = error.when(e(name).isNull() | (F.length(F.trim(e(name))) == 0), F.lit("invalid_" + name))
    error = (error.when(~e("vehicle_id").isin(sorted(vehicles())), "unknown_vehicle")
             .when(e("status").isNull() | ~e("status").isin("idle", "enroute", "on_trip"), "invalid_status")
             .when(e("fare_cents").isNull() | (e("fare_cents") < 0), "invalid_fare")
             .when(e("trip_completed").isNull(), "invalid_completion")
             .when(e("trip_completed") & (e("trip_id").isNull() | (F.length(F.trim(e("trip_id"))) == 0)), "missing_trip"))
    for name, low, high in (("lat", -90, 90), ("lon", -180, 180), ("speed_kmph", 0, 200)):
        error = error.when(e(name).isNull() | F.isnan(e(name)) | ~e(name).between(low, high), "invalid_" + name)
    for name in ("event_ts", "ingest_ts"):
        error = error.when(e(name).isNull() | ~e(name).rlike(r"(Z|[+-]\d{2}:\d{2})$") |
                           F.try_to_timestamp(e(name)).isNull(), "invalid_" + name)
    return error.otherwise(F.lit(None).cast("string"))
