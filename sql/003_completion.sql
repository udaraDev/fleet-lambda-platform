-- Additive upgrade. Old reports must be restated before being version-verified.
ALTER TABLE daily_report_status ADD COLUMN IF NOT EXISTS algorithm_version integer;
ALTER TABLE daily_report_status ADD COLUMN IF NOT EXISTS output_sha256 text;
ALTER TABLE stream_event_keys ADD COLUMN IF NOT EXISTS accepted_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS stream_event_keys_accepted_at ON stream_event_keys(accepted_at);
