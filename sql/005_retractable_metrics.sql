-- Migration 005: retain event contributions so conflict discovery can retract
-- an earlier event from its event-time window.

CREATE TABLE IF NOT EXISTS rt_zone_metric_events (
    event_id       text PRIMARY KEY,
    trip_id        text,
    fingerprint    text NOT NULL,
    window_start   timestamptz NOT NULL,
    zone           text NOT NULL,
    vehicle_id     text NOT NULL,
    status         text NOT NULL,
    trip_completed boolean NOT NULL,
    fare_cents     bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS rt_zone_metric_events_window
    ON rt_zone_metric_events (window_start, zone);
CREATE INDEX IF NOT EXISTS rt_zone_metric_events_trip
    ON rt_zone_metric_events (trip_id) WHERE trip_id IS NOT NULL;

ALTER TABLE rt_zone_metrics
    ADD COLUMN IF NOT EXISTS authoritative_corrected boolean NOT NULL DEFAULT false;
ALTER TABLE rt_zone_metrics
    ADD COLUMN IF NOT EXISTS reporting_vehicles integer NOT NULL DEFAULT 0;
