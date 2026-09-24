-- Migration 006: identify raw archive commits by their Kafka offset ranges.
-- Spark micro-batch IDs restart when a checkpoint is rebuilt, so they are
-- retained only as diagnostic metadata and are never an idempotency key.

ALTER TABLE pipeline_batches
    ADD COLUMN IF NOT EXISTS source_batch_id bigint;
ALTER TABLE pipeline_batches
    ADD COLUMN IF NOT EXISTS source_offsets jsonb;
ALTER TABLE pipeline_batches
    ADD COLUMN IF NOT EXISTS source_fingerprint text;

CREATE UNIQUE INDEX IF NOT EXISTS pipeline_batches_source_fingerprint
    ON pipeline_batches(source_fingerprint)
    WHERE source_fingerprint IS NOT NULL;

CREATE TABLE IF NOT EXISTS dq_archive_verification (
    batch_id bigint PRIMARY KEY REFERENCES pipeline_batches(batch_id) ON DELETE CASCADE,
    manifest_sha256 text NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT now()
);

-- Spark executors bulk-load here; the driver then performs one set-based,
-- transactional merge without materialising the micro-batch in Python.
CREATE UNLOGGED TABLE IF NOT EXISTS stream_serving_stage (
    run_id uuid NOT NULL,
    spark_batch_id bigint NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    event_id text NOT NULL,
    trip_id text,
    vehicle_id text NOT NULL,
    driver_id text NOT NULL,
    lat double precision NOT NULL,
    lon double precision NOT NULL,
    speed_kmph double precision NOT NULL,
    status text NOT NULL,
    fare_cents bigint NOT NULL,
    trip_completed boolean NOT NULL,
    zone text NOT NULL,
    event_ts timestamptz NOT NULL,
    ingest_ts timestamptz NOT NULL,
    time_of_day_bucket text NOT NULL,
    trace_id text NOT NULL,
    fingerprint text NOT NULL,
    staged_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id,kafka_partition,kafka_offset)
);
CREATE INDEX IF NOT EXISTS stream_serving_stage_event
    ON stream_serving_stage(run_id,event_id);
CREATE INDEX IF NOT EXISTS stream_serving_stage_trip
    ON stream_serving_stage(run_id,trip_id) WHERE trip_id IS NOT NULL;
