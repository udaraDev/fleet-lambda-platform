CREATE TABLE simulation_clock (
    id integer PRIMARY KEY CHECK (id = 1),
    started_at timestamptz NOT NULL DEFAULT now(),
    day_seconds integer NOT NULL CHECK (day_seconds > 0)
);

CREATE TABLE rt_vehicle_state (
    vehicle_id text PRIMARY KEY,
    zone text NOT NULL,
    status text NOT NULL CHECK (status IN ('idle', 'enroute', 'on_trip')),
    idle_since timestamptz,
    event_ts timestamptz NOT NULL,
    event_id text NOT NULL,
    trace_id text NOT NULL
);

CREATE TABLE completed_trips (
    trip_id text PRIMARY KEY,
    vehicle_id text NOT NULL,
    zone text NOT NULL,
    fare_cents bigint NOT NULL CHECK (fare_cents >= 0),
    completed_at timestamptz NOT NULL,
    time_of_day_bucket text NOT NULL,
    trace_id text NOT NULL
);
CREATE INDEX completed_trips_time ON completed_trips (completed_at);

CREATE TABLE pipeline_batches (
    batch_id bigint PRIMARY KEY,
    rows_in integer NOT NULL,
    rows_valid integer NOT NULL,
    rows_rejected integer NOT NULL,
    max_event_ts timestamptz,
    committed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE daily_vehicle_profit (
    dt date NOT NULL,
    vehicle_id text NOT NULL,
    trips integer NOT NULL,
    revenue_cents bigint NOT NULL,
    fuel_cents bigint,
    maintenance_cents bigint,
    profit_cents bigint,
    margin_pct numeric,
    is_unprofitable boolean,
    reconciliation_status text NOT NULL,
    PRIMARY KEY (dt, vehicle_id)
);

CREATE TABLE dq_quarantine (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL,
    dt date NOT NULL,
    source_file text NOT NULL,
    row_num integer NOT NULL,
    rule_failed text NOT NULL,
    raw_row jsonb NOT NULL
);

CREATE TABLE pipeline_runs (
    run_id uuid PRIMARY KEY,
    dt date NOT NULL,
    stage text NOT NULL,
    status text NOT NULL,
    rows_in integer,
    rows_out integer,
    error text,
    started_at timestamptz NOT NULL DEFAULT now(),
    ended_at timestamptz
);
