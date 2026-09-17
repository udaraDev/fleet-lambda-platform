# EC8203 Mini-Project — Comprehensive Project Plan

**Project:** Real-Time Ride-Hailing Fleet Operations & Profitability Platform
**Module:** EC8203 Applied Big Data Engineering (25% of grade, 2 weeks)
**Use Case:** #1 — Ride-Hailing Fleet Operations
**Architecture:** Lambda

---

## 1. Context

The **`EC8203 MiniProject 2026`** brief requires an end-to-end Lambda **or** Kappa pipeline with a streaming source + a daily-batch source, meaningful processing, a queryable serving store, a consolidated report/dashboard, and observability. It is marked out of 100, weighted heavily toward *architecture justification* (20) and *implementation* (40 combined).

Beyond the minimum rubric, this build emphasises production-style practices: data quality gates, end-to-end observability, performance analysis, and a clear mapping to managed cloud services.

**Intended outcome:** a Dockerised, reproducible repo + a 12-page report + a demo video.

---

## 2. Use Case & Business Requirements

**Scenario.** A ride-hailing operator needs (a) live visibility into fleet utilisation and earnings, and (b) a nightly reconciliation that tells them which vehicles are actually losing money once fuel and maintenance costs land.

**Business question.**
> What is fleet utilisation and earnings by zone/time-of-day right now, and which vehicles become unprofitable once yesterday's fuel/maintenance costs are factored in?

**Derived requirements**

| ID | Requirement | Latency | Consumer |
|----|-------------|---------|----------|
| R1 | Live utilisation: active vehicles, idle ratio, trips/hour, earnings by zone | < 1 min | Ops control room |
| R2 | Alert when a vehicle is idle beyond a threshold | < 1 min | Dispatcher |
| R3 | Daily per-vehicle profitability reconciliation (revenue − fuel − maintenance) | Next morning | Finance |
| R4 | Restated/corrected historical metrics when a cost file arrives late or is re-issued | Next batch run | Finance |
| R5 | Pipeline health must be observable and diagnosable | Continuous | Data engineering |

R1/R2 are **latency-critical, approximate-is-fine**. R3/R4 are **accuracy-critical, latency-tolerant, and must be restatable**. That split is the core of the architecture argument in §3.

---

## 3. Architecture Decision: Lambda (chosen) vs Kappa (rejected)

*This is the 20-mark section. It must be argued, not asserted.*

### Chosen: Lambda

Two paths over one immutable raw dataset:

- **Speed layer** — Spark Structured Streaming consumes `trip-events` from Kafka, computes windowed utilisation/earnings, writes to Postgres serving tables. Approximate, low-latency, may double-count a late event.
- **Batch layer** — Airflow, once per simulated day, reprocesses the *whole* raw Parquet dataset for that day, joins the vehicle-expense file, and recomputes authoritative per-vehicle revenue and profitability. Overwrites (restates) the day's serving table.
- **Serving layer** — Postgres holds both `rt_*` (speed) and `daily_*` (batch) tables; FastAPI reads whichever is appropriate per endpoint.

### Justification, tied to the four rubric axes

| Axis | Argument |
|------|----------|
| **Latency** | R1/R2 need sub-minute answers; R3 needs correctness by morning. One engine cannot optimise both without compromise. Lambda lets each path pick its own trade-off. |
| **Replay & correctness** | The expense file is an *external, correcting* feed. It arrives after the trips it prices, is occasionally re-issued with fixes, and changes the meaning of past data. A batch recompute over immutable raw Parquet gives clean restatement; the streaming path never has to model retroactive corrections. |
| **Consistency** | Finance needs exactly-once, reconcilable numbers. The batch layer is deterministic and re-runnable — re-running the same DAG on the same raw data yields the same result. The streaming path is explicitly labelled "indicative" in the API response. |
| **Cost** | The batch layer runs once per simulated day over a bounded partition; retaining raw Parquet is cheap. Kappa's alternative (long Kafka retention for full replay) is more expensive at real fleet scale. |

### Rejected: Kappa — and honestly why

Kappa (one streaming path, replay from the log to recompute) is genuinely attractive here: less code, one codebase, no dual-logic drift.

Rejected because:
1. **The batch source is not a stream.** Turning a daily CSV into a Kafka topic just to satisfy the paradigm is ceremony, not design.
2. **Retroactive restatement over a stream is hard.** A re-issued cost file means recomputing a closed window — awkward in Structured Streaming, trivial in a batch job.
3. **Full replay is the only recompute mechanism.** Any bug fix in profitability logic requires replaying the whole retained log, versus re-running one Airflow DAG.

**Acknowledged Lambda cost:** logic duplication between speed and batch layers, and the risk of the two drifting. **Mitigation:** shared transformation functions in `common/transforms.py` imported by both paths — the plan's structure enforces this. The report must state this honestly rather than pretending Lambda is free.

---

## 4. Technology Stack & Justification

| Layer | Technology | Justification (use-case-specific, not popularity) |
|-------|-----------|---------------------------------------------------|
| Ingestion | **Apache Kafka** (KRaft mode, single broker) | Durable replayable log; the raw Parquet writer and the aggregation job are two independent consumers of the same topic — a queue could not do this. Partitioning by `vehicle_id` guarantees per-vehicle ordering, needed for correct idle-duration state. |
| Topic design | `trip-events`, 3 partitions, key = `vehicle_id`, 7-day retention | 3 partitions demonstrates parallelism without overloading a laptop; keying preserves ordering where it matters. |
| Stream processing | **Spark Structured Streaming** (over Storm) | Same engine/API for both the speed layer and the batch layer, so transformation code is shared. Native event-time windowing + watermarks. Storm gives lower latency but has no batch story and no DataFrame API — irrelevant advantage at ~seconds-scale requirements. |
| Orchestration | **Apache Airflow** | Daily DAG with dependencies (sensor → validate → join → report → publish), retries, backfill for R4. Backfill is the concrete feature that makes restatement operational. |
| Raw store (data lake) | **Parquet on MinIO** (S3-compatible), partitioned `dt=YYYY-MM-DD/hour=HH` | Immutable master dataset — the foundation of Lambda. Columnar + partition pruning makes the daily recompute cheap. MinIO keeps the S3 API so the code ports to AWS S3 / Azure ADLS unchanged. |
| Serving store | **PostgreSQL** | Serving data is small, relational, and queried with joins/aggregates by a dashboard. Cassandra was considered and rejected: no ad-hoc joins, and the volume never justifies it. |
| API | **FastAPI** | Fulfils the "API endpoint" deliverable; auto OpenAPI docs make the demo self-explanatory. |
| Dashboard | **Grafana** (Postgres datasource) | Live panels + alert rules with no dashboard code to write; alerting and visualisation in one tool. |
| Metrics | **Prometheus** + `prometheus_client` | Scrapes custom pipeline counters; Grafana alert rules read from it. |
| Logging | **structlog** → JSON to stdout | Structured, greppable, carries a correlation ID across stages. |
| Data quality | **Great Expectations** (or a lightweight assertion module) | Validates the daily expense file before it can corrupt finance numbers. |
| Packaging | **Docker Compose** | Single-command reproducibility (rubric item), and the whole stack is portable. |

**Simulated clock:** 1 simulated day = 5 real minutes. Stated in the README, the report, and logged at startup by every service.

---

## 5. System Design

### 5.1 Data flow

```
producer_stream.py ──► Kafka: trip-events ──┬──► [raw_sink]  Spark job → Parquet on MinIO  (master dataset)
   (GPS/telemetry,                          │
    every 2s)                               └──► [speed]     Spark Structured Streaming
                                                              → 1-min tumbling windows, 2-min watermark
                                                              → Postgres  rt_zone_metrics
                                                                          rt_vehicle_state
                                                                          rt_alerts
producer_batch.py ──► MinIO: landing/expenses/expenses_<simday>.csv
   (1 file / 5 real min)                     │
                                             ▼
                              Airflow DAG  daily_profitability
                                  1 wait_for_expense_file   (S3KeySensor)
                                  2 validate_expense_file   (quality gate → quarantine)
                                  3 aggregate_trips         (Spark batch over Parquet for dt)
                                  4 join_costs              (revenue ⋈ expenses)
                                  5 compute_profitability   (margin, flags, ranking)
                                  6 publish_serving         (upsert → Postgres daily_vehicle_profit)
                                  7 render_report           (Parquet + CSV + HTML to reports/)
                                  8 emit_metrics            (push to Prometheus pushgateway)
                                             │
                                             ▼
                    Postgres ──► FastAPI ──► Grafana dashboard + daily report file
```

### 5.2 Schemas

**Stream event (`trip-events`)**
```json
{ "event_id":"uuid", "trip_id":"T-00042", "driver_id":"D-011", "vehicle_id":"V-007",
  "lat":6.9271, "lon":79.8612, "speed_kmph":34.2,
  "status":"idle|enroute|on_trip", "fare":420.50,
  "zone":"Z-COLOMBO-03", "event_ts":"2026-03-01T09:14:02Z",
  "ingest_ts":"2026-03-01T09:14:02.412Z", "trace_id":"..." }
```

**Daily batch file (`expenses_<simday>.csv`)**
```
vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag,report_date
```

**Postgres serving tables**

| Table | Written by | Key columns |
|-------|-----------|-------------|
| `rt_zone_metrics` | speed | `window_start, zone, active_vehicles, idle_ratio, trips, earnings` |
| `rt_vehicle_state` | speed | `vehicle_id, last_status, idle_since, last_seen_ts` |
| `rt_alerts` | speed | `alert_id, vehicle_id, type, severity, raised_at, payload` |
| `daily_vehicle_profit` | batch | `dt, vehicle_id, trips, revenue, fuel_cost, maintenance_cost, profit, margin_pct, is_unprofitable` |
| `daily_zone_summary` | batch | `dt, zone, trips, revenue, avg_utilisation` |
| `dq_quarantine` | batch | `dt, source_file, row_num, rule_failed, raw_row` |
| `pipeline_runs` | both | `run_id, stage, started_at, ended_at, status, rows_in, rows_out` |

### 5.3 Transformations (must be non-trivial — 15 marks)

**Speed layer**
- Parse + schema-enforce JSON; malformed events → `dead-letter` topic (not dropped silently).
- Deduplicate on `event_id` within the watermark window.
- Enrich: derive `time_of_day_bucket`, map lat/lon → zone if `zone` is null.
- Sessionise per vehicle: transitions between `idle`/`enroute`/`on_trip` to compute idle duration (stateful, `flatMapGroupsWithState` or equivalent).
- Aggregate: 1-minute tumbling window per zone → active vehicles, idle ratio, trips, earnings. 2-minute watermark for late events.
- Alert rule: vehicle `idle` continuously > N minutes → row in `rt_alerts`.

**Batch layer**
- Read the day's raw Parquet partition.
- Trip completion: collapse events to one row per `trip_id` (max fare, first/last timestamp, distance).
- Validate the expense file: non-negative costs, known `vehicle_id`, no duplicate vehicle rows, `distance_covered` within plausible bounds. Failures → `dq_quarantine`, and the DAG **fails loudly** if the failure rate exceeds a threshold.
- Join completed trips ⋈ expenses on `vehicle_id`; handle vehicles present in one source only (outer join + explicit null handling — a real reconciliation concern).
- Compute `profit = revenue − fuel_cost − maintenance_cost`, `margin_pct`, flag `is_unprofitable`, rank worst performers.
- Idempotent upsert into Postgres keyed on `(dt, vehicle_id)` so a re-run restates rather than duplicates.

### 5.4 Observability (10 marks)

- **Logging** — structlog JSON on every service; each event carries `trace_id` from producer → Kafka → Spark → Postgres, so one record is traceable end to end.
- **Metrics** (Prometheus): `events_produced_total`, `events_consumed_total`, `events_deadlettered_total`, `consumer_lag`, `batch_rows_in/out`, `dq_failed_rows_total`, `dag_duration_seconds`, `end_to_end_latency_seconds`.
- **Alert rules** (Grafana, minimum two — one pipeline, one business):
  1. *Pipeline health:* no events ingested in the last 2 minutes → pipeline down.
  2. *Data quality:* DQ failure rate > 5% on a daily file.
  3. *Business:* vehicle idle beyond threshold.
- **Health endpoints:** `GET /health` (liveness) and `GET /health/pipeline` (Kafka reachable, last event age, last successful DAG run).
- **Run ledger:** every stage writes to `pipeline_runs` — the audit trail a maintenance engineer actually needs.

### 5.5 API surface

| Endpoint | Purpose |
|----------|---------|
| `GET /metrics/fleet` | Live utilisation: active vehicles, idle ratio, trips/hour |
| `GET /metrics/zones?window=15m` | Earnings and utilisation by zone |
| `GET /alerts/active` | Currently raised alerts |
| `GET /reports/daily/{dt}` | Per-vehicle profitability for a simulated day |
| `GET /reports/daily/{dt}/unprofitable` | Ranked loss-making vehicles |
| `GET /health`, `GET /health/pipeline` | Health checks |
| `GET /metrics` | Prometheus scrape endpoint |

---

## 6. Repository Structure

```
fleet-lambda-platform/
├── docker-compose.yml
├── .env.example
├── Makefile                      # make up / make demo / make report / make test
├── README.md
├── docs/
│   ├── architecture.png          # main Lambda diagram
│   ├── dataflow.png
│   └── adr/
│       ├── 0001-lambda-over-kappa.md
│       ├── 0002-spark-over-storm.md
│       └── 0003-postgres-over-cassandra.md
├── config/
│   └── settings.yaml             # all thresholds, intervals, sim-clock — no magic numbers in code
├── simulators/
│   ├── producer_stream.py
│   ├── producer_batch.py
│   └── scenarios.py              # injects late events, dupes, bad rows, a vehicle going idle
├── common/
│   ├── schemas.py                # single source of truth for schemas
│   ├── transforms.py             # SHARED by speed + batch — prevents Lambda logic drift
│   ├── logging_conf.py
│   ├── metrics.py
│   └── db.py
├── streaming/
│   ├── raw_sink_job.py           # Kafka → Parquet (master dataset)
│   └── speed_layer_job.py        # Kafka → windowed aggregates → Postgres
├── batch/
│   ├── trip_aggregation.py
│   ├── expense_validation.py
│   └── profitability.py
├── airflow/dags/
│   ├── daily_profitability_dag.py
│   └── data_quality_dag.py
├── api/
│   ├── main.py
│   └── routers/
├── observability/
│   ├── prometheus.yml
│   └── grafana/{dashboards,alerts}/
├── sql/
│   ├── 001_schema.sql
│   └── 002_indexes.sql
├── tests/
│   ├── test_transforms.py        # pure functions on the shared logic
│   ├── test_expense_validation.py
│   └── test_api.py
└── reports/                      # generated daily outputs land here
```

---

## 7. Two-Week Execution Plan

| Day | Deliverable | Done when |
|-----|------------|-----------|
| **1** | Repo skeleton, `docker-compose.yml`, ADR 0001 (Lambda vs Kappa), architecture diagram v1 | `docker compose up` starts Kafka, Postgres, MinIO, Spark, Airflow, Grafana, Prometheus healthy |
| **2** | `config/settings.yaml`, `common/schemas.py`, `common/logging_conf.py`, SQL schema | Tables created on startup; structured logs visible |
| **3** | `producer_stream.py` + `scenarios.py` | Events flowing into `trip-events`; `kafka-console-consumer` shows them; late/dup/bad events injectable via flag |
| **4** | `producer_batch.py` writing to MinIO on the sim clock | One expense CSV appears per 5 real minutes; some files deliberately contain bad rows |
| **5** | `raw_sink_job.py` — Kafka → partitioned Parquet | Parquet partitions visible in MinIO console; row counts match produced counts |
| **6–7** | `speed_layer_job.py` — windows, watermark, dedup, idle sessionisation → Postgres | `rt_zone_metrics` and `rt_alerts` populate live; late events handled correctly |
| **8** | `batch/` modules + `test_transforms.py` | Batch job runs standalone on one Parquet partition and produces correct profitability |
| **9** | `daily_profitability_dag.py` wired end to end | DAG green in Airflow UI; `daily_vehicle_profit` populated; re-run restates, does not duplicate |
| **10** | Data quality gate + `dq_quarantine` + DAG failure on threshold breach | Deliberately bad file → rows quarantined, DAG fails, alert fires |
| **11** | FastAPI endpoints + `/metrics` + health checks | All endpoints return correct data; OpenAPI docs render |
| **12** | Grafana dashboard + Prometheus + 3 alert rules | Dashboard live; killing the producer fires the "no data" alert within 2 min |
| **13** | Report draft (all 7 required sections), README, screenshots | Report ≈12 pages with diagrams and real screenshots |
| **14** | Demo video (5–10 min), final polish, tests green, submission | `make demo` reproduces everything from a clean clone |

**Buffer strategy:** Days 6–7 (stateful streaming) and Day 9 (Airflow wiring) are the highest-risk. If either slips, cut scope in this order: (1) drop the second Airflow DAG, (2) simplify idle sessionisation to a simple last-seen timestamp comparison rather than full stateful processing, (3) reduce Grafana to one dashboard. **Never cut:** the architecture argument, the data quality gate, or observability — those carry the most marks.

---

## 8. Report Outline (8–15 pages, 15 marks)

1. **Introduction & use case** (1 p) — scenario, stakeholders, the business question.
2. **Requirements** (1 p) — the R1–R5 table from §2, with latency/consistency classification.
3. **Architecture decision** (2.5 p) — Lambda vs Kappa across latency, replay, consistency, cost; explicit rejection of Kappa with reasons; honest statement of Lambda's duplication cost and the shared-`transforms.py` mitigation. *Highest-value section — write it first, not last.*
4. **Architecture diagrams** (1.5 p) — layered view + data flow + Airflow DAG graph.
5. **Technology stack justification** (1.5 p) — the §4 table, each row tied to a requirement ID, with the alternative considered and rejected.
6. **Implementation** (2.5 p) — simulators, sim clock, transformation logic, watermarking/late data, idempotent upserts, data quality gate.
7. **Observability design** (1.5 p) — what is measured, how, why; screenshots of the dashboard and a firing alert; the trace-id walkthrough of one event end to end.
8. **Results** (1.5 p) — screenshots of the live dashboard, an API response, a sample daily profitability report, the quarantine table after a bad file.
9. **Limitations & production scale** (1.5 p) — single-broker Kafka, no exactly-once sink, no schema registry, no partition-level compaction; what changes at real scale (multi-broker + replication, Iceberg/Delta instead of raw Parquet, schema registry with Avro, autoscaling Spark, dbt for the serving models).
10. **Appendix** — cloud mapping table (§9), repo link, individual contributions if a group.

---

## 9. Cloud Mapping

For the report appendix — how each local component maps to a managed cloud service:

| This project | Azure | AWS |
|---|---|---|
| Kafka | Event Hubs (Kafka API) | MSK / Kinesis |
| Spark Structured Streaming | Databricks / Synapse Spark | EMR / Glue Streaming |
| Airflow | Data Factory / Managed Airflow | MWAA |
| MinIO + Parquet | ADLS Gen2 | S3 |
| PostgreSQL | Azure Database for PostgreSQL | RDS |
| Prometheus + Grafana | Azure Monitor | CloudWatch + Managed Grafana |

---

## 10. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|-----------|
| Spark + Kafka + Airflow together exhaust laptop RAM | High | Single Kafka broker in KRaft mode (no ZooKeeper), Spark in local mode, Airflow with LocalExecutor + SQLite metadata DB; document a 8 GB RAM minimum |
| Stateful streaming (idle sessionisation) proves fiddly | Medium | Fallback: derive idle duration from a `last_seen`/`last_status` table updated per micro-batch — simpler, still correct enough for the alert |
| Airflow + Spark container networking issues | Medium | Run the batch Spark job via `SparkSubmitOperator` against the same Spark container; keep a `PythonOperator` + local PySpark fallback |
| Two-week window slips | Medium | The cut-order in §7; the architecture argument and report are written from Day 1, not deferred |
| Cannot defend the code in the viva | Medium | ADR per major decision, written as the decision is made; a self-quiz pass on Day 13 covering every transformation |

---

## 11. Definition of Done

- [ ] `git clone` → `cp .env.example .env` → `make up` → `make demo` reproduces the full pipeline on a clean machine.
- [ ] Live dashboard shows zone utilisation updating within one minute of events.
- [ ] A daily profitability report file is generated per simulated day and readable via the API.
- [ ] Killing the stream producer fires the "no data" alert within 2 minutes.
- [ ] A deliberately corrupted expense file is quarantined and fails the DAG loudly.
- [ ] Re-running the DAG for the same day restates results without duplication.
- [ ] `pytest` green on transform and validation logic.
- [ ] Report PDF, 8–15 pages, covering all seven required sections with real screenshots.
- [ ] 5–10 minute demo video recorded.
- [ ] Every ADR written and every transformation explainable without notes.

---

## 12. Verification Plan

| What | How |
|------|-----|
| Ingestion correctness | Compare `events_produced_total` against `events_consumed_total` and Parquet row counts; they must reconcile |
| Late-event handling | Run the producer with `--inject-late`; confirm the affected window is updated within the watermark and dropped beyond it |
| Deduplication | Inject duplicate `event_id`s; confirm aggregate counts are unchanged |
| Batch correctness | Hand-compute profitability for 3 vehicles from the raw CSV and Parquet; assert against `daily_vehicle_profit` |
| Idempotency | Run the DAG twice for the same `dt`; row counts and values identical |
| Data quality gate | Feed a file with negative costs and an unknown `vehicle_id`; confirm quarantine rows and DAG failure |
| Observability | Stop the producer → "no data" alert fires; grep one `trace_id` across all service logs and see it at every stage |
| Serving layer | Hit every API endpoint; validate against a direct SQL query |
| Reproducibility | Fresh clone on a second machine, `make up`, full demo |
| Performance | Ramp producer to 10/100/500 eps; record end-to-end latency p50/p95 |
