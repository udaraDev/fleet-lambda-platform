# Implementation status

Latest: [priority correctness and Spark batch upgrade](PRIORITY_FIXES.md).
The sections below retain the historical first-increment implementation record;
the priority-fixes document supersedes its PyArrow-only batch and verification status.

## Increment 1: core pipeline

Implemented:

- Kafka KRaft broker and topic initialization with three vehicle-keyed partitions.
- Deterministic streaming telemetry and daily expense CSV simulators.
- Shared clock, event validation, money handling and accounting rules.
- Spark Structured Streaming ingestion, schema validation, time-of-day enrichment,
  micro-batch deduplication, Parquet archive and transactional live serving updates.
- PostgreSQL schema for current vehicles, unique completed trips, ingestion commits,
  daily profit, quarantine and batch run history.
- One Airflow DAG using LocalExecutor with PostgreSQL metadata (not SQLite).
- Batch validation and reconciliation; atomic replacement of each report date.
- FastAPI fleet/zone/time-of-day metrics, idle alerts, daily reports and health.
- Structured application logs and a no-data health rule with metrics export.
- Docker Compose startup configuration, local demo and automated tests.

## Differences from the full plan

This is a first increment toward PROJECT_PLAN.md. Local Parquet replaces MinIO
initially. One Spark query archives data and updates the speed layer. Batch uses
PyArrow/Python rather than Spark. A consolidated JSON report and its API endpoint
provide the first business output; Grafana is still pending.

The schema adds `trip_completed`, uses integer `fare_cents`, and makes missing-cost
profitability explicitly unknown. These decisions prevent counting every GPS ping
as a fare and avoid treating absent costs as zero.

## Validation

Verified on 2026-09-21:

- All 24 local business-rule, API and Parquet archive tests passed.
- Python syntax and Docker Compose configuration checks passed.
- Both application images built successfully, including Spark/Java/Kafka connector
  resolution and an actual Spark computation inside the image build.
- Kafka and PostgreSQL started healthy; all seven application tables initialized.
- Kafka topic and shared-volume initialization succeeded.
- Airflow initialized and its DAG import check reported no errors.
- The live producer published telemetry; Spark committed valid micro-batches into
  Parquet and PostgreSQL with no rejected rows in the observed sample.
- The API returned live metrics for all 12 vehicles from inside its container.

- After Docker recovery, the internal end-to-end smoke check passed for live
  health, fleet/zone metrics and the Airflow-generated 2026-03-01 daily report
  (12 unique vehicle results and correct profit arithmetic).
- Ten real PostgreSQL/Parquet integration checks passed: readiness, duplicate
  replay, expense-only losses, identical reruns, corrected costs, quarantine,
  preservation of good output, stale-row removal, missing-cost handling and
  concurrent same-day locking. Checks used an isolated UUID-named schema and
  temporary files; cleanup was confirmed and live report data was untouched.
- The live no-data fault test returned HTTP 503 after 124.05 seconds without
  valid ingestion (configured threshold: 120). Restarting the producer restored
  HTTP 200, with last-ingestion age 4.43 seconds.
- After the full Docker/WSL restart, Spark resumed its persisted batch sequence
  and committed new data (batch 175 observed). The host-side smoke check passed.
- Airflow's Windows-facing health endpoint reported healthy metadata storage and
  scheduler; its login page returned HTTP 200.

## Windows networking verification

Docker resumed successfully after the user's restart. Both web applications were
healthy inside Docker, but Windows localhost ports 8001 and 8080 timed out even
after recreating those two containers. WSL used mirrored networking. With user
approval, `.wslconfig` was backed up to
`C:\Users\User\.wslconfig.fleet-backup-20260921` and changed to NAT. After restarting
Docker/WSL, both host ports worked and the host-side end-to-end check passed.
This is a machine-wide WSL networking change, not a project configuration change.
No Docker data volumes, images or application data were deleted.

The simulation's wall clock continues through downtime, as documented in the
README. Expense files may therefore cover several additional simulated days after
resuming. This demo does not synthesize telemetry for the missed interval.

## Next milestones

1. Add a consolidated dashboard and business-friendly currency presentation.
2. Add Spark event-time windows, watermark/late-event tests and scenario injection.
3. Introduce MinIO and independent consumers after correctness is demonstrated.
4. Add Prometheus/Grafana monitoring, reproducible performance measurements,
   architecture diagrams and final report/demo evidence.
