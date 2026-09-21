# Fleet Lambda Platform

EC8203 mini-project: live ride-hailing fleet metrics and daily reconciliation of
trip revenue against fuel and maintenance expenses.

**Status: first implementation increment.** See
[implementation status](docs/IMPLEMENTATION_STATUS.md) for checks and remaining
work from the [full project plan](PROJECT_PLAN.md).

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
- Live fleet metrics: <http://localhost:8001/metrics/fleet>
- Zone metrics: <http://localhost:8001/metrics/zones>
- Time-of-day earnings: <http://localhost:8001/metrics/time-of-day?report_date=2026-03-01>
- Pipeline health: <http://localhost:8001/health/pipeline>
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

Airflow reprocesses all ready expense files every minute in this small demo, so
late events and corrected costs restate prior days. Readiness does not prove every
possible late event has arrived; reports reflect committed input at that run.
A failed quality check preserves the last good report.

Generated reports live in the shared volume. To copy one to the workspace:

```powershell
New-Item -ItemType Directory -Force reports
docker compose cp api:/data/reports/profitability_2026-03-01.json reports/
```

## Remaining work and limitations

Still planned: Spark event-time windows/watermarks, MinIO, separate raw/speed
consumers, scalable Spark batch aggregation, Grafana, Prometheus scraping/alerts,
fault-injection switches, durable business alerts and final report/demo evidence.
The current Airflow batch uses PyArrow and shared Python accounting rules.

This version uses one Kafka broker and single-node Spark. Micro-batches are bounded
and processed through the driver; daily reconciliation keeps a day's trips in
memory. It does not claim end-to-end exactly-once delivery. Simulation restarts may
skip ticks. Idle detection follows observed state changes and does not reconstruct
late historical sessions. Database and JSON publication are separate operations;
a failed export is retried by Airflow.

## Technical references

- [Kafka Docker](https://kafka.apache.org/39/getting-started/docker/)
- [Spark Kafka connector](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html)
- [Spark foreachBatch](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html)
- [Airflow container setup](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)
- [Airflow database requirements](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/set-up-database.html)

## Author

Udara Subodhitha Senevirathna, BSc Computer Engineering, University of Ruhuna.
