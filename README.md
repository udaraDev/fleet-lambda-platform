# Fleet Lambda Platform

EC8203 mini-project: live ride-hailing fleet metrics and daily reconciliation of
trip revenue against fuel and maintenance expenses.

**Status: submission candidate.** The delivered scope and deliberate deferrals are
recorded in [FINAL_SCOPE.md](docs/FINAL_SCOPE.md). PROJECT_PLAN.md is the original
proposal and is not a claim that every optional component was implemented.

The priority correctness upgrade is described in [priority fixes](docs/PRIORITY_FIXES.md).
Batch aggregation now uses Spark; missing telemetry is explicitly incomplete,
committed archives are verified, and serving writes use bounded transactional bulks.

### Upgrade an existing dataset

Fresh installations run all three SQL migration files automatically. Existing volumes need
the additive migration below before starting the upgraded writer. Back up the fleet
database first; do not delete checkpoints, archives or volumes.

```powershell
docker compose build api airflow-scheduler
docker compose stop airflow-scheduler streaming producer-stream
docker compose run --rm --no-deps api python -m scripts.migrate_integrity
docker compose up -d --no-deps streaming producer-stream producer-batch api airflow-scheduler airflow-webserver
```

The migration checks historical archive counts and baselines SHA-256 manifests and
event identities. Baseline hashes detect future changes, not alterations made before
the migration. If a file/count is missing, migration fails instead of inventing data.
Only restart upgraded writers after it succeeds.

## Architecture

```text
Python telemetry -> Kafka -> Spark Structured Streaming -> PostgreSQL live tables
                                      |
                                      +-> local Parquet archive
                                                    |
Python daily expense CSV -> Airflow reconciliation <-+
                                      |
                                      +-> PostgreSQL daily profitability
                                      +-> consolidated daily JSON report

PostgreSQL -> FastAPI: fleet metrics, daily reports, health and metrics export
```

The live path serves indicative operations metrics. The separate batch path
recomputes daily financial results from archived events and the expense feed.
Lambda supports corrected cost files while retaining original trip events.
Initially, one Spark query archives and updates live state; MinIO and separate
raw/speed consumers remain later milestones.

## Start on Windows PowerShell

Requirements: Docker Desktop running Linux containers, Docker Compose v2, and
internet for the first image build. Budget roughly 8 GB for Docker and measure
actual usage on your laptop. Containers use Python 3.11 and Java 17.

From the repository directory:

```powershell
Copy-Item .env.example .env
docker compose up --build -d
docker compose ps -a
docker compose logs -f streaming producer-stream airflow-scheduler
```

Do not overwrite an existing `.env`; defaults also work without copying the file.
The first build downloads large dependencies. Initialization services exit after
successful setup; this is expected. Other services should remain running.

- API documentation: <http://localhost:8001/docs>
- Business results page: <http://localhost:8001/>
- Live fleet metrics: <http://localhost:8001/metrics/fleet>
- Zone metrics: <http://localhost:8001/metrics/zones>
- Time-of-day earnings: <http://localhost:8001/metrics/time-of-day?report_date=2026-03-01>
- Pipeline health: <http://localhost:8001/health/pipeline>
- Batch/report health: <http://localhost:8001/health/reports>
- Metrics export: <http://localhost:8001/metrics>
- Available report dates: <http://localhost:8001/reports/daily>
- Airflow: <http://localhost:8080> (`admin` / `fleet_demo` by default).

Credentials are for a local classroom demo. Ports bind only to localhost; Kafka
and PostgreSQL are internal. If changing passwords in `.env`, use URL-safe
characters because connection URLs are assembled from those values.

The simulation starts when a producer first connects. After five real minutes,
the first expense file is published. Airflow polls once per real minute and waits
until Spark has committed data beyond the simulated day boundary. Allow startup
and processing time before checking the first report:

```powershell
Invoke-RestMethod http://localhost:8001/reports/daily/2026-03-01
docker compose exec api python -m batch.reconcile --date 2026-03-01
```

The second command restates that date. Identical inputs should give identical
rows and values. A database lock prevents overlapping runs for the same date.

## Demonstrate the no-data health rule

```powershell
docker compose stop producer-stream
# Wait 120 real seconds after Spark finishes its remaining input.
Invoke-WebRequest http://localhost:8001/health/pipeline
docker compose start producer-stream
```

Pipeline health returns HTTP 503 for no recent valid ingestion or database failure.
`/health` checks process liveness only. Notifications are not configured yet.

After startup, run a read-only check of live ingestion, daily output and profit
arithmetic (waits up to eight minutes for the first report):

```powershell
python -m scripts.verify_running --wait-seconds 480
```

If Windows port forwarding is unavailable, the same check can run inside Docker:

```powershell
docker compose exec -T api python -m scripts.verify_running --base-url http://127.0.0.1:8000 --wait-seconds 480
```

This verifies the application, not Windows/browser access. The latter requires
the host-side check to pass separately.

For real PostgreSQL/Parquet reconciliation checks, including corrected expenses,
duplicate trips, quarantine and preservation of the last good report:

```powershell
docker compose build api
docker compose run --rm --no-deps api python -m scripts.verify_reconciliation
```

This creates a uniquely named temporary database schema and removes it on exit;
live tables, expense files and reports are not modified. It needs the demo database
user's schema-creation permission.

The following optional fault test briefly stops the live telemetry producer,
checks HTTP 503 after the configured no-data threshold, restarts the producer in
a cleanup handler, and checks recovery to HTTP 200:

```powershell
python -m scripts.verify_no_data
```

Run it while the pipeline is healthy. If the terminal is forcibly terminated,
restore ingestion with `docker compose start producer-stream`.

## Tests and local sample

```powershell
python -m unittest discover -s tests -v
python -m scripts.demo_local
docker compose config --quiet
```

Business-rule tests and the sample reconciliation use Python's standard library.
API/archive tests run when dependencies exist and otherwise report a skip. The
sample is explicitly a business-logic demo, not a Kafka/Spark/Airflow integration
test. For all dependencies, use a Python 3.11 virtual environment:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
.\.venv\Scripts\python -m unittest discover -s tests -v
```

## Data and accounting rules

- Events, database tables and reports use integer **LKR cents**. Divide by 100 to
  display LKR. Expense CSVs accept decimal LKR values.
- A fare is counted only when `trip_completed` is true, once per `trip_id`.
  Repeated GPS events are not trips. Conflicting completion records fail processing.
- A trip belongs to its UTC completion date, including trips crossing midnight.
- Missing expense records produce `missing_expenses` and null profitability.
  Expense-only vehicles have zero revenue and retain their costs.
- Missing or partial telemetry produces `incomplete_telemetry`; conflicting events
  produce `conflicting_events`. Profit, margin and loss classification are unknown
  in both cases. Coverage checks the registered fleet's configured simulated cadence.
- Daily API responses include a publication run ID, export status and per-vehicle
  coverage. A pending export explicitly indicates that the file has not caught up
  with the database version. Legacy reports without metadata remain unverified.
- Every fourth vehicle stays parked, providing an idle-alert example and a
  loss-making vehicle with costs but no completed trips.
- Active means reporting in `enroute` or `on_trip`. Idle ratio is idle/reporting
  vehicles. Reporting vehicles were observed within 30 simulated minutes of the
  newest processed event. Live trip counts and earnings cover the last simulated
  hour. Check pipeline health before interpreting potentially stale values.
- The persisted clock starts at `2026-03-01T00:00:00Z`. One simulated day equals
  300 real seconds. Business times use simulated UTC; health and scheduling use
  real time. A two-real-second tick advances 9.6 simulated minutes by default.
- Keep clock scale, event interval and fleet size stable for a dataset's lifetime.
  Downtime advances the clock and can leave telemetry gaps.
- Invalid stream records are archived under `/data/quarantine/stream`. Bad CSV
  rows go to `dq_quarantine`; more than 5% rejected rows stops daily publication.

## Persistence and recovery

Named volumes hold PostgreSQL, Kafka, Parquet, checkpoints and Airflow logs.
`docker compose down` stops/removes containers without deleting this data.
`docker compose up -d` resumes the stack.

Each Spark batch writes Parquet before transactionally publishing live data and
a commit ledger entry. Retries rewrite only an uncommitted batch. Reconciliation
reads ledger-committed archives. Never delete only the checkpoint, archive or
database: these form one dataset.

Airflow checks ready dates every minute using a committed input snapshot, newest
first. Unchanged verified inputs skip Spark recomputation; changed/late inputs
restate their date. A failed date does not prevent other dates from processing,
and the overall task fails after collecting per-date errors. Missing expected
expense files are errors. A failed quality/archive check preserves the last good
report. Financial completeness does not imply that no future correction can arrive.
Each run recomputes at most five changed dates by default, deferring remaining
historical backfills so newly closed days get another scheduling opportunity.
Set `MAX_CHANGED_DATES_PER_RUN` to tune that budget; unchanged checks do not consume it.

Generated reports live in the shared volume. To copy one to the workspace:

```powershell
New-Item -ItemType Directory -Force reports
docker compose cp api:/data/reports/profitability_2026-03-01.json reports/
```

## Deliberate limits

Formal Spark event-time windows/watermarks, MinIO, separate raw/speed consumers,
Grafana/Prometheus servers, durable business-alert history and a distributed sink
are explicitly deferred in FINAL_SCOPE.md. They are not described as completed.
The Airflow batch uses Spark DataFrames for trip aggregation, conflicts, coverage
and the expense join. PyArrow is used for archive metadata and test fixtures.

This version uses one Kafka broker and single-node Spark. Micro-batches are bounded
and capped at 2,500 rows before collection; serving uses bulk database operations
inside one transaction with the commit ledger. Daily trip state stays in Spark,
with only fleet-size summaries collected. It does not claim end-to-end exactly-once delivery. Simulation restarts may
skip ticks. Idle detection follows observed state changes and does not reconstruct
late historical sessions. Database and JSON publication are separate operations;
a pending publication status exposes export failure until Airflow retries it.
Published JSON is versioned and SHA-256 verified; missing or corrupted output is
regenerated. Use [DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md) for the prepared live demo.

## Technical references

- [Kafka Docker](https://kafka.apache.org/39/getting-started/docker/)
- [Spark Kafka connector](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html)
- [Spark foreachBatch](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html)
- [Airflow container setup](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)
- [Airflow database requirements](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/set-up-database.html)

## Author

Udara Subodhitha Senevirathna, BSc Computer Engineering, University of Ruhuna.
