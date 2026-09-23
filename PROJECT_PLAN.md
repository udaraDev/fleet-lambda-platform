# EC8203 Mini-Project — Original Comprehensive Project Plan

> Historical proposal. For the delivered architecture, verified features and
> deliberate deferrals, use `docs/FINAL_SCOPE.md`. Code and documentation items are
> implemented; the recording, personal rehearsal and final upload remain student actions.
>

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

## 7. Team Structure & Responsibilities

The work is split **by pipeline layer**, so each member owns a complete, demonstrable part of the system and can defend it in the viva. Replace *Member A/B/C* with names.

| Member | Role | Owns (code) | Rubric areas led |
|--------|------|-------------|------------------|
| **Member A** | Platform & Ingestion Engineer | `docker-compose.yml`, `.env.example`, `Makefile`, `config/`, `simulators/`, `streaming/raw_sink_job.py`, `observability/`, `common/logging_conf.py`, `common/metrics.py` | Data Ingestion (15), Observability (10), Code Quality & Reproducibility (5) |
| **Member B** | Stream Processing & Serving Engineer | `common/schemas.py`, `common/transforms.py`, `streaming/speed_layer_job.py`, `sql/`, `common/db.py`, `api/` | Processing: speed layer (15, shared), Storage & Serving (10) |
| **Member C** | Batch Processing & Data Quality Engineer | `batch/`, `airflow/dags/`, data quality rules, `dq_quarantine`, daily report rendering | Processing: batch layer (15, shared), Report compilation (15) |

### Shared responsibilities (all members)

- **Architecture decision (20 marks):** Lambda vs Kappa argument agreed in a joint session on Day 1; each member contributes the justification for their own layer.
- **Tech stack justification (10 marks):** each member writes the rows for the tools they own.
- **Tests:** each member writes unit tests for their own modules.
- **Demo video:** each member presents their own layer (about 2-3 minutes each).
- **Viva readiness:** each member reviews at least one other member's pull request, so every line is understood by two people.

### Report section ownership

| Report section | Lead | Support |
|----------------|------|---------|
| 1-2 Introduction, use case, requirements | C | All |
| 3 Architecture decision (Lambda vs Kappa) | All (joint) | - |
| 4 Architecture diagrams | A | B |
| 5 Tech stack justification | Each writes own rows | C edits |
| 6 Implementation: ingestion | A | - |
| 6 Implementation: speed layer & serving | B | - |
| 6 Implementation: batch layer & data quality | C | - |
| 7 Observability design | A | - |
| 8 Results & screenshots | B | All |
| 9 Limitations & production scale | All | C edits |
| Final compilation, formatting, PDF | C | - |

### Integration contracts (agreed on Day 2, before parallel work starts)

These interfaces let the three tracks work independently without blocking each other:

| Contract | Defined by | Consumed by |
|----------|-----------|-------------|
| Kafka event schema (`trip-events`) in `common/schemas.py` | B (with A) | A (producer), B (speed layer), C (batch reads Parquet) |
| Expense CSV schema + MinIO landing path | C (with A) | A (batch simulator), C (DAG) |
| Raw Parquet layout `dt=YYYY-MM-DD/hour=HH` | A | C |
| Postgres serving tables in `sql/001_schema.sql` | B | B (speed, API), C (batch upserts), A (Grafana) |
| Metric names + log fields in `common/metrics.py`, `common/logging_conf.py` | A | B, C |

Until a dependency is ready, each member works against **sample fixtures** (a small JSON event file, a sample Parquet partition, a sample CSV) committed to `tests/fixtures/`.

### Collaboration workflow

- **Branches:** `main` is always runnable; each member works on `feature/<area>-<short-name>` branches.
- **Pull requests:** every merge into `main` needs one review from another member.
- **Stand-up:** 10-minute daily check-in: done yesterday, doing today, blocked by.
- **Task board:** GitHub Projects board with one card per deliverable in §8, assigned to its owner.
- **Contribution evidence:** commits and PRs are the record used for the contribution statement.

### Individual contribution statement (submission template)

> **Member A (Platform & Ingestion):** Designed and built the Docker Compose environment, streaming and batch data simulators with fault injection, the Kafka topic design, the raw Parquet sink, and the observability stack (structured logging, Prometheus metrics, Grafana dashboards and alert rules). Wrote the report sections on diagrams, ingestion and observability.
>
> **Member B (Stream Processing & Serving):** Designed the shared event schemas and transformation library, implemented the Spark Structured Streaming speed layer (windowing, watermarking, deduplication, idle detection), the PostgreSQL serving schema, and the FastAPI serving layer. Wrote the report sections on the speed layer, serving layer and results.
>
> **Member C (Batch Processing & Data Quality):** Implemented the Airflow daily profitability DAG, Spark batch aggregation and cost reconciliation logic, the data quality gate with row quarantine, idempotent serving loads, and daily report generation. Wrote the report sections on requirements and the batch layer, and compiled the final report.
>
> **Joint:** Architecture decision (Lambda vs Kappa), technology stack justification, integration testing, and the demo video.

---

## 8. Two-Week Execution Plan

Three parallel tracks. Rows marked **Joint** are team-wide milestones.

### Week 1: Foundations and core pipelines

| Day | Member A: Platform & Ingestion | Member B: Stream & Serving | Member C: Batch & Data Quality |
|-----|--------------------------------|-----------------------------|---------------------------------|
| 1 | **Joint:** agree Lambda decision, draft ADR 0001, architecture diagram v1, set up GitHub board | **Joint** | **Joint** |
| 2 | Repo skeleton, `docker-compose.yml` (Kafka, MinIO, Postgres, Spark, Airflow) | `common/schemas.py`, `sql/001_schema.sql` | Expense CSV schema, data quality rule list, Airflow container setup |
| 2 (end) | **Joint:** sign off integration contracts (§7), commit sample fixtures | **Joint** | **Joint** |
| 3 | `producer_stream.py` + `config/settings.yaml` | `common/transforms.py` (pure functions) + unit tests | `batch/trip_aggregation.py` against fixture Parquet |
| 4 | `scenarios.py` (late, duplicate, bad events) + `producer_batch.py` | `speed_layer_job.py`: parse, schema enforcement, dead-letter topic | `batch/expense_validation.py` + `test_expense_validation.py` |
| 5 | `raw_sink_job.py`: Kafka to partitioned Parquet | Speed layer: 1-min windows, watermark, dedup to `rt_zone_metrics` | `batch/profitability.py`: join, profit, flags + tests |
| 6 | `common/logging_conf.py` + `common/metrics.py`; add logging to simulators | Speed layer: idle detection to `rt_vehicle_state`, `rt_alerts` | `daily_profitability_dag.py` skeleton (sensor, validate, aggregate) |
| 7 | **Joint integration checkpoint:** events flow producer to Kafka to Parquet + Postgres; DAG reads real Parquet | **Joint** | **Joint** |

### Week 2: Serving, observability, integration and delivery

| Day | Member A: Platform & Ingestion | Member B: Stream & Serving | Member C: Batch & Data Quality |
|-----|--------------------------------|-----------------------------|---------------------------------|
| 8 | Prometheus setup, scrape configs, pushgateway | FastAPI: `/metrics/fleet`, `/metrics/zones`, `/alerts/active` | DAG: join, profitability, idempotent upsert to `daily_vehicle_profit` |
| 9 | Grafana dashboards (live fleet + pipeline health) | FastAPI: `/reports/daily/*`, `/health`, `/health/pipeline` | DQ gate: quarantine table, DAG fails above threshold |
| 10 | Grafana alert rules (no data, DQ failure, idle vehicle) | `test_api.py`, `pipeline_runs` logging, Postgres read-only role | Daily report rendering (CSV/HTML) + `data_quality_dag.py` |
| 11 | **Joint integration day:** full end-to-end run, fix cross-layer bugs, run §13 verification checks, each member walks the others through their layer | **Joint** | **Joint** |
| 12 | `Makefile`, README setup steps, clean-clone reproducibility test, performance benchmark | Capture result screenshots; write report sections (speed layer, serving, results) | Write report sections (intro, requirements, batch, DQ); start compiling |
| 13 | Write report sections (diagrams, ingestion, observability); review C's code | Review A's report sections and code | Compile full report; review B's sections |
| 14 | **Joint:** record demo video, final review, contribution statement, submit | **Joint** | **Joint** |

**Buffer strategy:** the highest-risk items are Member B's stateful idle detection (Day 6) and Member C's Airflow wiring (Days 8-9). If either slips, the other members help on integration day, and scope is cut in this order: (1) drop the second Airflow DAG, (2) simplify idle detection to a last-seen timestamp comparison, (3) reduce Grafana to one dashboard. **Never cut:** the architecture argument, the data quality gate, or observability. Those carry the most marks.

---

## 9. Report Outline (8–15 pages, 15 marks)

1. **Introduction & use case** (1 p) — scenario, stakeholders, the business question.
2. **Requirements** (1 p) — the R1–R5 table from §2, with latency/consistency classification.
3. **Architecture decision** (2.5 p) — Lambda vs Kappa across latency, replay, consistency, cost; explicit rejection of Kappa with reasons; honest statement of Lambda's duplication cost and the shared-`transforms.py` mitigation. *Highest-value section — write it first, not last.*
4. **Architecture diagrams** (1.5 p) — layered view + data flow + Airflow DAG graph.
5. **Technology stack justification** (1.5 p) — the §4 table, each row tied to a requirement ID, with the alternative considered and rejected.
6. **Implementation** (2.5 p) — simulators, sim clock, transformation logic, watermarking/late data, idempotent upserts, data quality gate.
7. **Observability design** (1.5 p) — what is measured, how, why; screenshots of the dashboard and a firing alert; the trace-id walkthrough of one event end to end.
8. **Results** (1.5 p) — screenshots of the live dashboard, an API response, a sample daily profitability report, the quarantine table after a bad file.
9. **Limitations & production scale** (1.5 p) — single-broker Kafka, no exactly-once sink, no schema registry, no partition-level compaction; what changes at real scale (multi-broker + replication, Iceberg/Delta instead of raw Parquet, schema registry with Avro, autoscaling Spark, dbt for the serving models).
10. **Appendix** — cloud mapping table (§10), repo link, individual contribution statement (§7).

---

## 10. Cloud Mapping

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

## 11. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|-----------|
| Spark + Kafka + Airflow together exhaust laptop RAM | High | Single Kafka broker in KRaft mode (no ZooKeeper), Spark in local mode, Airflow with LocalExecutor + SQLite metadata DB; document a 8 GB RAM minimum |
| Stateful streaming (idle sessionisation) proves fiddly | Medium | Fallback: derive idle duration from a `last_seen`/`last_status` table updated per micro-batch — simpler, still correct enough for the alert |
| Airflow + Spark container networking issues | Medium | Run the batch Spark job via `SparkSubmitOperator` against the same Spark container; keep a `PythonOperator` + local PySpark fallback |
| Two-week window slips | Medium | The cut-order in §8; the architecture argument and report are written from Day 1, not deferred |
| One track blocks another | Medium | Integration contracts signed off on Day 2; each member develops against committed sample fixtures until the real upstream is ready |
| Uneven contribution or knowledge silos | Medium | Clear ownership in §7; mandatory cross-member PR review; each member presents their own layer in the demo |
| Cannot defend the code in the viva | Medium | ADR per major decision, written as the decision is made; each member walks the others through their layer on Day 11 |

---

## 12. Definition of Done

- [x] The isolated clean-install workflow reproduces the full pipeline from fresh named volumes on the verified Docker host and removes only its disposable volumes.
- [x] Live dashboard shows zone utilisation updating within one minute of events.
- [x] A daily profitability report file is generated per simulated day and readable via the API.
- [x] Killing the stream producer fires the "no data" alert within 2 minutes.
- [x] A deliberately corrupted expense file is quarantined and fails the DAG loudly.
- [x] Re-running the DAG for the same day restates results without duplication.
- [x] `pytest` green on transform and validation logic.
- [x] Report PDF, 8–15 pages, covering all seven required sections with real screenshots.
- [x] An approximately eight-minute live-demo runbook is prepared; the brief permits a live demonstration instead of a recorded video.
- [x] Every implemented architecture decision is covered by an ADR, and the transformation contracts are documented for viva preparation.
- [x] An individual contribution statement records sole authorship; the three-member template does not apply to this individual submission.
- [x] Ten likely viva questions and evidence-based answers are prepared in `docs/VIVA_QA.md`.

The actual live presentation (or optional recording) and the final portal upload remain student-performed submission actions, not implementation tasks.

---

## 13. Verification Plan

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
