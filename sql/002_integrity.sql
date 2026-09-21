-- Additive and repeatable upgrade; no existing events or reports are deleted.
ALTER TABLE pipeline_batches ADD COLUMN IF NOT EXISTS archive_manifest jsonb;
CREATE TABLE IF NOT EXISTS stream_event_keys (
    event_id text PRIMARY KEY,
    fingerprint text NOT NULL,
    event_ts timestamptz NOT NULL,
    vehicle_id text NOT NULL,
    trip_id text,
    batch_id bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_conflicts (
    batch_id bigint NOT NULL,
    event_id text NOT NULL,
    dt date NOT NULL,
    reason text NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (batch_id, event_id, reason)
);
CREATE INDEX IF NOT EXISTS stream_conflicts_date ON stream_conflicts(dt);
CREATE INDEX IF NOT EXISTS stream_conflicts_event ON stream_conflicts(event_id);
CREATE INDEX IF NOT EXISTS stream_conflicts_trip ON stream_conflicts((payload->>'trip_id'));
CREATE TABLE IF NOT EXISTS daily_report_status (
    dt date PRIMARY KEY,
    run_id uuid NOT NULL,
    input_fingerprint text NOT NULL,
    coverage jsonb NOT NULL,
    export_status text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
