"""Fixed business identity contract shared by speed and batch processing.

Ingest time and derived columns are not identity. Event timestamps compare as UTC
instants. Numeric types come from the fixed Parquet/stream schema.
"""

EVENT_FIELDS = ("event_id", "trip_id", "driver_id", "vehicle_id", "lat", "lon", "speed_kmph",
                "status", "fare_cents", "trip_completed", "zone", "event_ts", "trace_id")
