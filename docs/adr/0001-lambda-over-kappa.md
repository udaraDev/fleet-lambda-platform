# ADR 0001: Lambda Architecture Over Kappa

**Date:** 2026-03-01  
**Status:** Accepted  
**Deciders:** Udara Subodhitha Senevirathna  

---

## Context

The EC8203 brief requires an end-to-end big data pipeline with a streaming source,
a daily-batch source, meaningful processing, a queryable serving layer, and
observability. The two dominant architectural patterns are Lambda and Kappa.

The use case is a ride-hailing fleet operator with two distinct data consumers:

- **Operations** (R1/R2): needs live vehicle utilisation and idle alerts with
  sub-minute latency. Approximate is acceptable.
- **Finance** (R3/R4): needs authoritative daily per-vehicle profitability, fully
  reconciled against a separate expense file that can arrive late or be reissued
  with corrections. Accuracy is mandatory; latency tolerance is next-morning.

The expense file is a **daily CSV, not a stream of events.** It arrives once per
simulated day, is occasionally re-issued with corrected values, and changes the
meaning of past trip data without producing new trip events.

---

## Decision

**Choose Lambda Architecture.**

Two independent processing paths over one shared, immutable raw dataset:

- **Speed layer:** Spark Structured Streaming consumes `trip-events` from Kafka,
  writes live vehicle state and completed-trip records to PostgreSQL. Results are
  labelled "indicative" in the API.
- **Batch layer:** Airflow schedules Spark to reprocess the day's committed Parquet
  archive, join the expense file, and publish authoritative per-vehicle profitability.
  Re-running the same DAG on the same inputs produces byte-identical output.
- **Serving layer:** PostgreSQL holds both `rt_*` (speed path) and
  `daily_vehicle_profit` (batch path); FastAPI serves whichever is appropriate.

---

## Justification — Four Rubric Axes

| Axis | Argument |
|---|---|
| **Latency** | R1/R2 need sub-minute answers. R3 needs correctness by morning. One engine cannot optimise both without compromise. Lambda lets each path choose its own trade-off independently. |
| **Replay and correctness** | The expense file is an external, correcting feed. It arrives after the trips it prices, is occasionally re-issued with fixes, and changes the meaning of past data. Batch recompute over immutable Parquet gives clean, auditable restatement. The streaming path never has to model retroactive corrections. |
| **Consistency** | Finance requires exactly-reproducible numbers. The batch layer is deterministic: same raw archive + same expense file + same algorithm version = identical row values. A SHA-256 export fingerprint and an algorithm version field verify this. The streaming path is explicitly labelled "indicative" in every API response. |
| **Cost** | The batch layer runs once per simulated day over one bounded Parquet partition. Kappa's alternative — long Kafka retention for full log replay — is more expensive at real fleet scale and more operationally complex to reason about when a cost file changes. |

---

## Rejected Alternative: Kappa

Kappa (single streaming path; replay from the log to recompute) was seriously
considered. It has genuine advantages: less code, one codebase, no dual-logic drift.

**Rejected for three concrete reasons:**

1. **The batch source is not a stream.** Turning a daily CSV into a Kafka topic just
   to satisfy the paradigm is ceremony, not design. The expense file has no natural
   event semantics — it replaces, not appends.

2. **Retroactive restatement over a stream is hard.** A re-issued cost file means
   recomputing a closed window — this requires keyed state retractions or full
   log replay in Structured Streaming, but is trivial in a batch job (DELETE the
   old rows, recompute, INSERT the corrected rows).

3. **Full replay is the only recompute mechanism.** Any bug fix in profitability
   logic requires replaying the entire retained Kafka log. Lambda re-runs one
   Airflow DAG over a bounded Parquet partition with the corrected code.

---

## Consequences

### Positive
- Clean separation of latency and consistency requirements.
- Batch restatement is simple, auditable, and deterministic.
- Each layer can be scaled, replaced, or debugged independently.

### Negative (acknowledged)
- **Logic duplication:** domain.py:reconcile() and spark_reconcile.py:aggregate()
  implement equivalent business logic in two runtimes. A formula change must be
  applied to both.
- **Two-path drift risk:** mitigated by shared event schema, explicit ALGORITHM_VERSION,
  and a Spark/Python parity test in tests/test_completion.py.
- **Eventual consistency:** live metrics are "indicative"; daily results are
  "authoritative". The API labels them accordingly.
- **Raw/serving coupling:** one Spark job handles both archive and live state in this
  implementation. A production system would use separate consumers.
