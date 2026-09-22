"""Bounded bulk serving writes in the same transaction as the commit ledger.

No connection or query per event. Retain one ordered state fold per bounded
micro-batch; scaling past this cap requires partition staging plus a final merge.
"""

import hashlib
import json
from collections import defaultdict

from psycopg2.extras import Json, execute_values

from common.domain import parse_timestamp

from common.events import EVENT_FIELDS


def fingerprint(item):
    canonical = {k: item[k] for k in EVENT_FIELDS}
    canonical["event_ts"] = parse_timestamp(canonical["event_ts"]).isoformat()
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()


def bulk_serve(cur, events, batch_id):
    if not events:
        return 0
    if len(events) > 2500:
        raise ValueError("Serving batch exceeds bounded driver cap")
    ids = list({e["event_id"] for e in events})
    trip_ids = list({e["trip_id"] for e in events if e["trip_id"]})
    cur.execute("SELECT event_id,fingerprint,event_ts,vehicle_id,trip_id FROM stream_event_keys WHERE event_id=ANY(%s)", (ids,))
    prior_events = {r[0]: r[1:] for r in cur.fetchall()}
    known = {key: value[0] for key, value in prior_events.items()}
    trip_ids = list(set(trip_ids) | {v[3] for v in prior_events.values() if v[3]})
    cur.execute("SELECT trip_id,vehicle_id,fare_cents,completed_at,zone FROM completed_trips WHERE trip_id=ANY(%s)", (trip_ids,))
    old_trips = {r[0]: r[1:] for r in cur.fetchall()}
    cur.execute("SELECT event_id,payload FROM stream_conflicts WHERE event_id=ANY(%s) OR payload->>'trip_id'=ANY(%s)", (ids, trip_ids))
    blocked_events, blocked_trips = set(), set()
    for event_id, payload in cur.fetchall():
        blocked_events.add(event_id)
        if payload.get("trip_id"):
            blocked_trips.add(payload["trip_id"])
    by_id = defaultdict(list)
    for event in events:
        by_id[event["event_id"]].append(event)
    conflicting_ids = {key for key, copies in by_id.items()
                       if len({fingerprint(e) for e in copies} | ({known[key]} if key in known else set())) > 1}
    conflicting_ids |= blocked_events
    signatures = defaultdict(set)
    for event in events:
        if event["trip_completed"]:
            signatures[event["trip_id"]].add((event["vehicle_id"], event["fare_cents"],
                                             parse_timestamp(event["event_ts"]), event["zone"]))
    bad_trips = {trip for trip, values in signatures.items()
                if len(values | ({old_trips[trip]} if trip in old_trips else set())) > 1}
    bad_trips |= blocked_trips | {e["trip_id"] for e in events if e["event_id"] in conflicting_ids and e["trip_id"]}
    bad_trips |= {prior_events[key][3] for key in conflicting_ids & prior_events.keys() if prior_events[key][3]}
    rejected, accepted = [], []
    for key in conflicting_ids & prior_events.keys():
        _, ts, vehicle, trip = prior_events[key]
        rejected.append((batch_id, key, ts.date(), "prior_event_conflict",
                         Json({"vehicle_id": vehicle, "trip_id": trip})))
    for event_id, copies in by_id.items():
        item = copies[0]
        if event_id in conflicting_ids or item["trip_id"] in bad_trips:
            # Preserve every distinct conflict payload; raw archive retains all copies.
            for index, item in enumerate(copies):
                rejected.append((batch_id, event_id, parse_timestamp(item["event_ts"]).date(),
                                 f"conflicting_event_or_trip_{index}", Json(item)))
        elif event_id not in known:
            accepted.append(item)
    for trip in bad_trips & old_trips.keys():
        vehicle, fare, ts, zone = old_trips[trip]
        rejected.append((batch_id, "prior:" + trip, ts.date(), "prior_trip_conflict",
                         Json({"trip_id": trip, "vehicle_id": vehicle, "fare_cents": fare})))
    if rejected:
        execute_values(cur, """INSERT INTO stream_conflicts(batch_id,event_id,dt,reason,payload)
                       VALUES %s ON CONFLICT DO NOTHING""", rejected)
    if bad_trips:
        cur.execute("DELETE FROM completed_trips WHERE trip_id=ANY(%s)", (list(bad_trips),))
    if conflicting_ids or bad_trips:
        # Unknown is safer than continuing to display a discredited observation.
        # A new accepted observation reintroduces the vehicle; never replay a bad state.
        cur.execute("""DELETE FROM rt_vehicle_state WHERE event_id=ANY(%s) OR event_id IN
            (SELECT event_id FROM stream_event_keys WHERE trip_id=ANY(%s))""",
            (list(conflicting_ids), list(bad_trips)))
    if not accepted:
        return len(rejected)
    cur.execute("""SELECT vehicle_id,zone,status,idle_since,event_ts,event_id,trace_id
                   FROM rt_vehicle_state WHERE vehicle_id=ANY(%s)""", (list({e["vehicle_id"] for e in accepted}),))
    states = {r[0]: r for r in cur.fetchall()}
    changed, trips = {}, {}
    for item in sorted(accepted, key=lambda e: (parse_timestamp(e["event_ts"]), e["event_id"])):
        vehicle, ts = item["vehicle_id"], parse_timestamp(item["event_ts"])
        old = states.get(vehicle)
        if old is None or (ts, item["event_id"]) > (old[4], old[5]):
            idle = (old[3] if old and old[2] == "idle" else ts) if item["status"] == "idle" else None
            states[vehicle] = (vehicle, item["zone"], item["status"], idle, ts, item["event_id"], item["trace_id"])
            changed[vehicle] = states[vehicle]
        if item["trip_completed"]:
            trips[item["trip_id"]] = (item["trip_id"], vehicle, item["zone"], item["fare_cents"], ts,
                                      item["time_of_day_bucket"], item["trace_id"])
    if changed:
        execute_values(cur, """INSERT INTO rt_vehicle_state
            (vehicle_id,zone,status,idle_since,event_ts,event_id,trace_id) VALUES %s
            ON CONFLICT(vehicle_id) DO UPDATE SET zone=EXCLUDED.zone,status=EXCLUDED.status,
            idle_since=EXCLUDED.idle_since,event_ts=EXCLUDED.event_ts,event_id=EXCLUDED.event_id,
            trace_id=EXCLUDED.trace_id""", list(changed.values()))
    if trips:
        execute_values(cur, """INSERT INTO completed_trips
            (trip_id,vehicle_id,zone,fare_cents,completed_at,time_of_day_bucket,trace_id)
            VALUES %s ON CONFLICT DO NOTHING""", list(trips.values()))
    execute_values(cur, """INSERT INTO stream_event_keys
        (event_id,fingerprint,event_ts,vehicle_id,trip_id,batch_id) VALUES %s ON CONFLICT DO NOTHING""",
        [(e["event_id"], fingerprint(e), e["event_ts"], e["vehicle_id"], e["trip_id"], batch_id) for e in accepted])
    return len(rejected)
