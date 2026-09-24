-- Migration 007: archive continuity and complete publication manifests.

CREATE TABLE IF NOT EXISTS archive_partition_offsets (
    topic text NOT NULL,
    partition integer NOT NULL,
    next_offset bigint NOT NULL CHECK (next_offset >= 0),
    batch_id bigint NOT NULL REFERENCES pipeline_batches(batch_id) ON DELETE RESTRICT,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

-- Existing installations may already have thousands of committed source ranges.
-- Recovery batches describe a broad Kafka snapshot rather than one contiguous raw
-- micro-batch, so exclude them from the writer high-water mark.
INSERT INTO archive_partition_offsets(topic, partition, next_offset, batch_id)
SELECT DISTINCT ON (topic, partition)
       topic, partition, next_offset, batch_id
FROM (
    SELECT COALESCE(b.source_offsets->>'topic', 'trip-events') AS topic,
           p.key::integer AS partition,
           (p.value->>'end')::bigint AS next_offset,
           b.batch_id
    FROM pipeline_batches b
    CROSS JOIN LATERAL jsonb_each(b.source_offsets->'partitions') p
    WHERE b.source_offsets ? 'partitions'
      AND NOT (b.source_offsets ? 'recovery')
) ranges
ORDER BY topic, partition, next_offset DESC, batch_id DESC
ON CONFLICT(topic, partition) DO UPDATE SET
    next_offset = GREATEST(archive_partition_offsets.next_offset, EXCLUDED.next_offset),
    batch_id = CASE
        WHEN EXCLUDED.next_offset >= archive_partition_offsets.next_offset
        THEN EXCLUDED.batch_id ELSE archive_partition_offsets.batch_id END,
    updated_at = now();

ALTER TABLE daily_report_status
    ADD COLUMN IF NOT EXISTS export_manifest jsonb;
