"""Partition staging and set-based serving merge for Spark micro-batches."""

from psycopg2.extras import execute_values

from common.db import connection
from common.settings import IDLE_ALERT_MINUTES
from streaming.sink import fingerprint


def stage_partition(rows, run_id, batch_id):
    """Bulk-load one Spark partition without retaining it on the driver."""
    values = []
    for row in rows:
        item = row.asDict(recursive=True)
        values.append((
            run_id, batch_id, item.pop("partition"), item.pop("offset"),
            item["event_id"], item.get("trip_id"), item["vehicle_id"],
            item["driver_id"], item["lat"], item["lon"], item["speed_kmph"],
            item["status"], item["fare_cents"], item["trip_completed"],
            item["zone"], item["event_ts"], item["ingest_ts"],
            item["time_of_day_bucket"], item["trace_id"], fingerprint(item),
        ))
        if len(values) >= 500:
            _write(values)
            values.clear()
    if values:
        _write(values)


def _write(values):
    with connection() as conn, conn.cursor() as cur:
        execute_values(cur, """INSERT INTO stream_serving_stage
            (run_id,spark_batch_id,kafka_partition,kafka_offset,event_id,trip_id,
             vehicle_id,driver_id,lat,lon,speed_kmph,status,fare_cents,
             trip_completed,zone,event_ts,ingest_ts,time_of_day_bucket,trace_id,fingerprint)
            VALUES %s ON CONFLICT DO NOTHING""", values, page_size=500)


def dead_letter_partition(rows, batch_id):
    """Publish invalid rows from one executor partition without driver collection."""
    from itertools import chain

    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return
    from kafka import KafkaProducer
    from common.settings import KAFKA_BOOTSTRAP
    from streaming.sink import dead_letter_send
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, retries=5,
                             api_version_auto_timeout_ms=10000)
    try:
        with connection() as conn, conn.cursor() as cur:
            for row in chain((first,), iterator):
                dead_letter_send(producer, row["raw"], row["error"],
                                 row["partition"], row["offset"], batch_id, cur)
            producer.flush()
    finally:
        producer.close()


def metrics_partition(rows):
    """Bulk-upsert already aggregated Spark window rows per executor partition."""
    values = [(r["window_start"], r["zone"], r["reporting"],
               r["active_vehicles"], r["idle_ratio"], r["trips"],
               r["earnings_cents"]) for r in rows]
    if not values:
        return
    with connection() as conn, conn.cursor() as cur:
        execute_values(cur, """INSERT INTO rt_zone_metrics
            (window_start,zone,reporting_vehicles,active_vehicles,idle_ratio,trips,earnings_cents)
            VALUES %s ON CONFLICT(window_start,zone) DO UPDATE SET
            reporting_vehicles=EXCLUDED.reporting_vehicles,
            active_vehicles=EXCLUDED.active_vehicles,idle_ratio=EXCLUDED.idle_ratio,
            trips=EXCLUDED.trips,earnings_cents=EXCLUDED.earnings_cents,updated_at=now()
            WHERE NOT rt_zone_metrics.authoritative_corrected""", values, page_size=500)


def merge_staged(cur, run_id, batch_id):
    """Merge a staged micro-batch using bounded metadata and SQL set operations."""
    cur.execute("SELECT pg_advisory_xact_lock(8203,0)")
    cur.execute("DELETE FROM stream_serving_stage WHERE staged_at < now()-interval '1 day'")
    cur.execute("""CREATE TEMP TABLE stage_conflict_ids ON COMMIT DROP AS
        SELECT event_id FROM stream_serving_stage WHERE run_id=%s
        GROUP BY event_id HAVING count(DISTINCT fingerprint)>1
        UNION
        SELECT s.event_id FROM stream_serving_stage s JOIN stream_event_keys k USING(event_id)
        WHERE s.run_id=%s AND s.fingerprint<>k.fingerprint
        UNION
        SELECT s.event_id FROM stream_serving_stage s JOIN stream_conflicts c USING(event_id)
        WHERE s.run_id=%s""", (run_id, run_id, run_id))
    cur.execute("CREATE UNIQUE INDEX ON stage_conflict_ids(event_id)")

    cur.execute("""CREATE TEMP TABLE stage_bad_trips ON COMMIT DROP AS
        SELECT trip_id FROM stream_serving_stage WHERE run_id=%s AND trip_completed
        GROUP BY trip_id HAVING count(DISTINCT jsonb_build_array(
            vehicle_id,fare_cents,event_ts,zone))>1
        UNION
        SELECT s.trip_id FROM stream_serving_stage s JOIN completed_trips t USING(trip_id)
        WHERE s.run_id=%s AND s.trip_completed AND
            (s.vehicle_id,s.fare_cents,s.event_ts,s.zone)<>
            (t.vehicle_id,t.fare_cents,t.completed_at,t.zone)
        UNION
        SELECT s.trip_id FROM stream_serving_stage s JOIN stage_conflict_ids c USING(event_id)
        WHERE s.run_id=%s AND s.trip_id IS NOT NULL
        UNION
        SELECT k.trip_id FROM stream_event_keys k JOIN stage_conflict_ids c USING(event_id)
        WHERE k.trip_id IS NOT NULL
        UNION
        SELECT c.payload->>'trip_id' FROM stream_conflicts c
        WHERE c.payload->>'trip_id' IN
            (SELECT trip_id FROM stream_serving_stage WHERE run_id=%s AND trip_id IS NOT NULL)""",
        (run_id, run_id, run_id, run_id))
    cur.execute("CREATE UNIQUE INDEX ON stage_bad_trips(trip_id)")

    cur.execute("""INSERT INTO stream_conflicts(batch_id,event_id,dt,reason,payload)
        SELECT %s,s.event_id,s.event_ts::date,
            'staged_conflict_'||s.kafka_partition||'_'||s.kafka_offset,
            jsonb_build_object('event_id',s.event_id,'trip_id',s.trip_id,
                'vehicle_id',s.vehicle_id,'fare_cents',s.fare_cents,
                'event_ts',s.event_ts,'zone',s.zone)
        FROM stream_serving_stage s
        WHERE s.run_id=%s AND (EXISTS
            (SELECT 1 FROM stage_conflict_ids c WHERE c.event_id=s.event_id) OR EXISTS
            (SELECT 1 FROM stage_bad_trips b WHERE b.trip_id=s.trip_id))
        ON CONFLICT DO NOTHING""", (batch_id, run_id))
    cur.execute("""INSERT INTO stream_conflicts(batch_id,event_id,dt,reason,payload)
        SELECT %s,k.event_id,k.event_ts::date,'prior_event_conflict',
            jsonb_build_object('vehicle_id',k.vehicle_id,'trip_id',k.trip_id)
        FROM stream_event_keys k JOIN stage_conflict_ids c USING(event_id)
        ON CONFLICT DO NOTHING""", (batch_id,))
    cur.execute("""INSERT INTO stream_conflicts(batch_id,event_id,dt,reason,payload)
        SELECT %s,'prior:'||t.trip_id,t.completed_at::date,'prior_trip_conflict',
            jsonb_build_object('trip_id',t.trip_id,'vehicle_id',t.vehicle_id,
                'fare_cents',t.fare_cents)
        FROM completed_trips t JOIN stage_bad_trips b USING(trip_id)
        ON CONFLICT DO NOTHING""", (batch_id,))

    cur.execute("""CREATE TEMP TABLE stage_affected_windows ON COMMIT DROP AS
        SELECT DISTINCT e.window_start,e.zone FROM rt_zone_metric_events e
        WHERE EXISTS(SELECT 1 FROM stage_conflict_ids c WHERE c.event_id=e.event_id)
           OR EXISTS(SELECT 1 FROM stage_bad_trips b WHERE b.trip_id=e.trip_id)
        UNION
        SELECT DISTINCT date_trunc('minute',s.event_ts),s.zone
        FROM stream_serving_stage s WHERE s.run_id=%s AND
            (EXISTS(SELECT 1 FROM stage_conflict_ids c WHERE c.event_id=s.event_id)
             OR EXISTS(SELECT 1 FROM stage_bad_trips b WHERE b.trip_id=s.trip_id))""", (run_id,))
    cur.execute("DELETE FROM completed_trips WHERE trip_id IN (SELECT trip_id FROM stage_bad_trips)")
    cur.execute("""DELETE FROM rt_vehicle_state WHERE event_id IN
        (SELECT event_id FROM stage_conflict_ids UNION
         SELECT event_id FROM stream_event_keys WHERE trip_id IN
            (SELECT trip_id FROM stage_bad_trips))""")
    cur.execute("""DELETE FROM rt_zone_metric_events WHERE event_id IN
        (SELECT event_id FROM stage_conflict_ids) OR trip_id IN
        (SELECT trip_id FROM stage_bad_trips)""")

    cur.execute("""CREATE TEMP TABLE stage_accepted ON COMMIT DROP AS
        SELECT DISTINCT ON(s.event_id) s.* FROM stream_serving_stage s
        WHERE s.run_id=%s
          AND NOT EXISTS(SELECT 1 FROM stage_conflict_ids c WHERE c.event_id=s.event_id)
          AND NOT EXISTS(SELECT 1 FROM stage_bad_trips b WHERE b.trip_id=s.trip_id)
          AND NOT EXISTS(SELECT 1 FROM stream_event_keys k WHERE k.event_id=s.event_id)
        ORDER BY s.event_id,s.event_ts,s.kafka_partition,s.kafka_offset""", (run_id,))
    cur.execute("CREATE UNIQUE INDEX ON stage_accepted(event_id)")

    cur.execute("""INSERT INTO rt_zone_metric_events
        (event_id,trip_id,fingerprint,window_start,zone,vehicle_id,status,trip_completed,fare_cents)
        SELECT event_id,trip_id,fingerprint,date_trunc('minute',event_ts),zone,
            vehicle_id,status,trip_completed,fare_cents FROM stage_accepted
        ON CONFLICT(event_id) DO NOTHING""")
    cur.execute("""INSERT INTO rt_zone_metrics
        (window_start,zone,reporting_vehicles,active_vehicles,idle_ratio,
         trips,earnings_cents,authoritative_corrected)
        SELECT a.window_start,a.zone,
            count(DISTINCT e.vehicle_id),
            count(DISTINCT e.vehicle_id) FILTER(WHERE e.status<>'idle'),
            count(DISTINCT e.vehicle_id) FILTER(WHERE e.status='idle')::numeric /
                NULLIF(count(DISTINCT e.vehicle_id),0),
            count(e.event_id) FILTER(WHERE e.trip_completed),
            COALESCE(sum(e.fare_cents) FILTER(WHERE e.trip_completed),0),true
        FROM stage_affected_windows a LEFT JOIN rt_zone_metric_events e
          ON e.window_start=a.window_start AND e.zone=a.zone
        GROUP BY a.window_start,a.zone
        ON CONFLICT(window_start,zone) DO UPDATE SET
            reporting_vehicles=EXCLUDED.reporting_vehicles,
            active_vehicles=EXCLUDED.active_vehicles,idle_ratio=EXCLUDED.idle_ratio,
            trips=EXCLUDED.trips,earnings_cents=EXCLUDED.earnings_cents,
            authoritative_corrected=true,updated_at=now()""")

    cur.execute("""CREATE TEMP TABLE stage_state_candidates ON COMMIT DROP AS
        WITH bounds AS (
            SELECT vehicle_id,
                max(event_ts) FILTER(WHERE status<>'idle') AS last_non_idle,
                min(event_ts) FILTER(WHERE status='idle') AS first_idle
            FROM stage_accepted GROUP BY vehicle_id
        ), latest AS (
            SELECT DISTINCT ON(vehicle_id) * FROM stage_accepted
            ORDER BY vehicle_id,event_ts DESC,event_id DESC
        )
        SELECT l.vehicle_id,l.zone,l.status,
            CASE WHEN l.status<>'idle' THEN NULL
                 WHEN b.last_non_idle IS NULL AND old.status='idle'
                    THEN LEAST(old.idle_since,b.first_idle)
                 ELSE (SELECT min(a.event_ts) FROM stage_accepted a
                       WHERE a.vehicle_id=l.vehicle_id AND a.status='idle'
                       AND (b.last_non_idle IS NULL OR a.event_ts>b.last_non_idle)) END AS idle_since,
            l.event_ts,l.event_id,l.trace_id
        FROM latest l JOIN bounds b USING(vehicle_id)
        LEFT JOIN rt_vehicle_state old USING(vehicle_id)""")
    cur.execute("""INSERT INTO rt_vehicle_state
        (vehicle_id,zone,status,idle_since,event_ts,event_id,trace_id)
        SELECT vehicle_id,zone,status,idle_since,event_ts,event_id,trace_id
        FROM stage_state_candidates
        ON CONFLICT(vehicle_id) DO UPDATE SET zone=EXCLUDED.zone,status=EXCLUDED.status,
            idle_since=EXCLUDED.idle_since,event_ts=EXCLUDED.event_ts,
            event_id=EXCLUDED.event_id,trace_id=EXCLUDED.trace_id
        WHERE (EXCLUDED.event_ts,EXCLUDED.event_id)>
              (rt_vehicle_state.event_ts,rt_vehicle_state.event_id)""")
    cur.execute("""INSERT INTO completed_trips
        (trip_id,vehicle_id,zone,fare_cents,completed_at,time_of_day_bucket,trace_id)
        SELECT trip_id,vehicle_id,zone,fare_cents,event_ts,time_of_day_bucket,trace_id
        FROM stage_accepted WHERE trip_completed
        ON CONFLICT DO NOTHING""")
    cur.execute("""INSERT INTO stream_event_keys
        (event_id,fingerprint,event_ts,vehicle_id,trip_id,batch_id)
        SELECT event_id,fingerprint,event_ts,vehicle_id,trip_id,%s FROM stage_accepted
        ON CONFLICT DO NOTHING""", (batch_id,))

    cur.execute("""WITH reference AS (SELECT max(event_ts) AS ts FROM stage_accepted)
        INSERT INTO rt_alerts(vehicle_id,alert_type,severity,idle_minutes,payload)
        SELECT s.vehicle_id,'idle_threshold','warning',
            EXTRACT(EPOCH FROM(reference.ts-s.idle_since))/60,
            jsonb_build_object('idle_since',s.idle_since)
        FROM stage_state_candidates s CROSS JOIN reference
        WHERE s.idle_since IS NOT NULL
          AND EXTRACT(EPOCH FROM(reference.ts-s.idle_since))/60 >= %s
          AND NOT EXISTS(SELECT 1 FROM rt_alerts a WHERE a.vehicle_id=s.vehicle_id
              AND a.alert_type='idle_threshold' AND a.resolved_at IS NULL)""",
        (IDLE_ALERT_MINUTES,))
    cur.execute("""UPDATE rt_alerts a SET resolved_at=now()
        FROM stage_state_candidates s WHERE a.vehicle_id=s.vehicle_id
          AND a.alert_type='idle_threshold' AND a.resolved_at IS NULL
          AND s.status<>'idle'""")

    cur.execute("SELECT count(*) FROM stage_conflict_ids")
    conflicts = cur.fetchone()[0]
    cur.execute("DELETE FROM stream_serving_stage WHERE run_id=%s", (run_id,))
    return conflicts
