-- Migration 004: Tables from the original PROJECT_PLAN.md that were deferred.
-- Safe to re-run (IF NOT EXISTS on all objects).

-- Windowed zone metrics written by the speed layer.
-- One row per (window_start, zone) — overwritten each micro-batch.
CREATE TABLE IF NOT EXISTS rt_zone_metrics (
    window_start  timestamptz NOT NULL,
    zone          text        NOT NULL,
    active_vehicles integer   NOT NULL DEFAULT 0,
    idle_ratio    numeric,
    trips         integer     NOT NULL DEFAULT 0,
    earnings_cents bigint     NOT NULL DEFAULT 0,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);

-- Persistent alert history. rt_vehicle_state shows current idle state;
-- this table accumulates every idle-threshold breach with timestamps.
CREATE TABLE IF NOT EXISTS rt_alerts (
    alert_id      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_id    text        NOT NULL,
    alert_type    text        NOT NULL CHECK (alert_type IN ('idle_threshold', 'no_telemetry')),
    severity      text        NOT NULL CHECK (severity IN ('warning', 'critical')),
    raised_at     timestamptz NOT NULL DEFAULT now(),
    resolved_at   timestamptz,
    idle_minutes  numeric,
    payload       jsonb       NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS rt_alerts_vehicle ON rt_alerts (vehicle_id, raised_at DESC);
CREATE INDEX IF NOT EXISTS rt_alerts_unresolved ON rt_alerts (raised_at) WHERE resolved_at IS NULL;

-- Daily per-zone batch summary written by the reconciliation job.
CREATE TABLE IF NOT EXISTS daily_zone_summary (
    dt            date        NOT NULL,
    zone          text        NOT NULL,
    trips         integer     NOT NULL DEFAULT 0,
    revenue_cents bigint      NOT NULL DEFAULT 0,
    vehicles      integer     NOT NULL DEFAULT 0,
    avg_utilisation numeric,
    PRIMARY KEY (dt, zone)
);
CREATE INDEX IF NOT EXISTS daily_zone_summary_dt ON daily_zone_summary (dt);

-- Dead-letter log: malformed Kafka events that could not be parsed or validated.
-- The Kafka dead-letter topic is the primary store; this table mirrors rejected
-- payloads for SQL-level diagnostics without requiring a separate Kafka consumer.
CREATE TABLE IF NOT EXISTS stream_dead_letters (
    id            bigserial   PRIMARY KEY,
    batch_id      bigint,
    kafka_partition integer,
    kafka_offset  bigint,
    raw_value     text        NOT NULL,
    error_reason  text        NOT NULL,
    received_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dead_letters_batch ON stream_dead_letters (batch_id);
