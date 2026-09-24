# Run the Fleet Lambda Platform

This repository implements the EC8203 ride-hailing use case as a local Lambda architecture. It combines live fleet telemetry with daily expense files to answer two questions: what is happening across the fleet now, and which vehicles are profitable after fuel and maintenance costs?

The project is designed for a localhost classroom demonstration. It is not configured for public or production deployment.

## What the platform implements

The delivered system includes:

- **Streaming ingestion**: a Python producer sends telemetry for 12 vehicles to a three-partition Apache Kafka topic
- **Speed layer**: Spark Structured Streaming validates events, applies a two-minute watermark, updates live vehicle state, raises idle alerts, and writes one-minute zone windows
- **Raw layer**: an independent Spark consumer writes partitioned Parquet to MinIO and commits row-count and SHA-256 manifests
- **Daily source**: a Python producer uploads one expense CSV per five-minute simulated day
- **Batch layer**: Apache Airflow invokes PySpark reconciliation over verified raw archives and the latest valid expense file
- **Serving layer**: PostgreSQL stores live state, window metrics, alerts, daily profitability, publication metadata, quarantine rows, and run history
- **Output layer**: FastAPI serves a business page and query endpoints; reconciliation publishes JSON, CSV, HTML, and Parquet reports
- **Observability**: structured JSON logs, Prometheus metrics and rules, Grafana dashboards, health endpoints, and Airflow data-quality checks

One simulated day equals 300 real seconds. The clock begins at `2026-03-01T00:00:00Z`. Money is stored as integer LKR cents.

## Understand the architecture

The speed and batch paths serve different consistency needs:

```text
Python telemetry -> Kafka -> Spark speed consumer -> PostgreSQL live tables
                         |
                         +-> Spark raw consumer -> MinIO Parquet and manifests
                                                     |
Python expense CSV -> Airflow -> Spark batch <--------+
                                  |
                                  +-> PostgreSQL daily tables
                                  +-> versioned report files

PostgreSQL -> FastAPI -> business page and API
FastAPI -> Prometheus -> Grafana
```

The speed path reports indicative operational metrics. The batch path recomputes authoritative daily results from committed archives. This separation supports corrected expense files without changing the original telemetry archive.

Read [the final scope](docs/FINAL_SCOPE.md) for the delivered architecture and deliberate deferrals. `PROJECT_PLAN.md` records the original proposal and traceability history.

## Meet the prerequisites

Install the following software before starting:

- Docker Desktop with Linux containers
- Docker Compose v2
- PowerShell 7 for the commands below
- Python 3.11 for host-side tests and verification scripts
- At least 8 GB of memory available to Docker
- Internet access for the first image build

The first build downloads Kafka, Spark, Airflow, MinIO, PostgreSQL, Prometheus, Grafana, Java, and Python dependencies.

## Start the platform

Run these commands from the repository root:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up --build -d
docker compose ps -a
```

Do not replace an existing `.env`. Compose also works with the documented defaults when `.env` is absent.

Initialization containers exit after creating databases, migrations, buckets, and Kafka topics. Their successful exit is expected. The API, Airflow, Kafka, MinIO, PostgreSQL, producers, Prometheus, Grafana, and two Spark consumers remain running.

Follow the main processing logs with:

```powershell
docker compose logs -f streaming-raw streaming-speed producer-stream airflow-scheduler
```

## Open the interfaces

Use these localhost endpoints after the containers start:

- [Business results page](http://localhost:8001/)
- [FastAPI documentation](http://localhost:8001/docs)
- [Live fleet metrics](http://localhost:8001/metrics/fleet)
- [Fifteen-minute zone metrics](http://localhost:8001/metrics/zones?window=15)
- [Active idle alerts](http://localhost:8001/alerts/active?idle_minutes=15)
- [Available daily reports](http://localhost:8001/reports/daily)
- [Example daily profitability report](http://localhost:8001/reports/daily/2026-03-01)
- [Pipeline health](http://localhost:8001/health/pipeline)
- [Report health](http://localhost:8001/health/reports)
- [Prometheus metrics](http://localhost:8001/metrics)
- [Airflow](http://localhost:8080), using `admin` and `fleet_demo`
- [MinIO console](http://localhost:9001), using `minioadmin` and `minioadmin`
- [Prometheus](http://localhost:9090)
- [Grafana](http://localhost:3000), using `admin` and `fleet_demo`

The credentials support a localhost demonstration only. All published ports bind to `127.0.0.1`. Kafka and PostgreSQL are not exposed to the host.

## Wait for the first daily report

The first expense file appears after five real minutes. Airflow checks for closed simulated dates once per minute. Spark then reads the relevant archive partitions and publishes the daily result.

Check report health before opening a daily report:

```powershell
Invoke-RestMethod http://localhost:8001/health/reports
Invoke-RestMethod http://localhost:8001/reports/daily/2026-03-01
```

Report health may return `processing` while Spark reconciles one newly closed date. It returns HTTP 503 when reports fall further behind or when publication, data quality, version, archive-lag, or run-status checks fail.

Restate one date manually with:

```powershell
docker compose exec api python -m batch.reconcile --date 2026-03-01
```

Identical inputs produce identical values. A database advisory lock prevents overlapping reconciliation for the same date.

## Verify the implementation

Run the host suite first:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-dev.txt
.\.venv\Scripts\python -m pytest tests -q -rs
docker compose config --quiet
```

The verified host result is 54 passed tests, 17 passed subtests, and one dependency-gated Spark parity skip.

Run the skipped parity test inside the application image, where PySpark is installed:

```powershell
docker compose run --rm --no-deps api python -m unittest tests.test_completion.SparkPythonParityTests -v
```

Run the isolated PostgreSQL, Spark, Parquet, publication, and conflict suite with:

```powershell
docker compose run --rm --no-deps api python -m scripts.verify_reconciliation
```

The script creates a UUID-named PostgreSQL schema and an isolated MinIO verification prefix. It removes both after the run and does not modify live serving tables or archives. The verified result contains 25 named checks.

Validate the running platform with:

```powershell
python -m scripts.verify_running --wait-seconds 480
```

If Windows cannot reach the forwarded port, run the same application check inside Docker:

```powershell
docker compose exec -T api python -m scripts.verify_running --base-url http://127.0.0.1:8000 --wait-seconds 480
```

The container check validates the application path. It does not prove that Windows or a host browser can reach the forwarded port.

## Verify a clean installation

The clean-install verifier starts a separate Compose project with empty, uniquely named volumes and alternate localhost ports. It verifies initialization, ingestion, MinIO, APIs, monitoring, Airflow imports, and a daily report. It then removes only its disposable containers and volumes.

Run it when ports `18001`, `18080`, `19000`, `19001`, `19090`, and `13000` are free:

```powershell
python -m scripts.verify_clean_install
```

The saved evidence validates fresh volumes on the same Docker host. It does not claim an uncached installation on a second computer.

## Demonstrate failure detection

Run the automated no-data exercise while pipeline health is green:

```powershell
python -m scripts.verify_no_data
```

The script stops only the telemetry producer, waits for the 120-second threshold, confirms HTTP 503, restarts the producer in a cleanup handler, and confirms recovery to HTTP 200.

If the terminal is terminated before cleanup, restart ingestion with:

```powershell
docker compose start producer-stream
```

Use [the demo runbook](docs/DEMO_RUNBOOK.md) for the complete eight-minute presentation sequence.

## Interpret the business results

Apply these rules when reading live and daily outputs:

- **Fare recognition**: revenue counts only records where `trip_completed` is true, once per `trip_id`
- **Trip date**: a trip belongs to its UTC completion date
- **Missing costs**: `missing_expenses` produces null profit, margin, and loss classification
- **Missing telemetry**: `incomplete_telemetry` keeps profitability unknown instead of treating missing events as zero revenue
- **Conflicts**: `conflicting_events` invalidates affected profitability until the source history is rebuilt through an audited correction
- **Expense-only vehicles**: registered vehicles remain visible with zero observed revenue and their submitted costs
- **Live activity**: active means `enroute` or `on_trip`; reporting vehicles were observed within 30 simulated minutes of the newest accepted event
- **Data quality**: more than 5% rejected expense rows fails that date and preserves the last good publication
- **Quarantine**: invalid stream records enter MinIO quarantine, Kafka dead letters, and `stream_dead_letters`; invalid expense rows enter `dq_quarantine`

Check `/health/pipeline` before interpreting live values. A running interface can still display stale business data after an ingestion failure.

## Preserve the dataset contract

Named volumes persist PostgreSQL, Kafka, MinIO objects, Spark checkpoints, Airflow logs, Prometheus data, and Grafana state.

```powershell
docker compose down
docker compose up -d
```

`docker compose down` removes containers and networks but preserves named volumes. Do not delete only a checkpoint, archive prefix, Kafka volume, or database volume. These components form one logical dataset.

Keep the fleet size, simulation start, tick interval, and clock scale unchanged for the lifetime of an existing dataset. Downtime advances the simulated clock and can create honest telemetry gaps.

## Upgrade an existing dataset

Fresh installations apply all six ordered fleet migrations automatically. Existing persistent volumes require the additive migration before upgraded writers start.

Back up the database, then run:

```powershell
docker compose build api airflow-scheduler
docker compose stop airflow-scheduler streaming-raw streaming-speed producer-stream
docker compose run --rm --no-deps api python -m scripts.migrate_integrity
docker compose up -d --no-deps streaming-raw streaming-speed producer-stream producer-batch api airflow-scheduler airflow-webserver
```

The migration validates historical archive counts and creates baseline manifests and event identities. It fails when required evidence is missing instead of creating replacement data.

## Recover archive data cautiously

Use recovery commands only after identifying an exact damaged batch or event-time gap. Stop `streaming-raw` before changing archive state.

For a legacy batch with a retained local Parquet copy, run:

```powershell
docker compose stop streaming-raw
docker compose run --rm --no-deps api python -m scripts.restore_archive_batch 1234567890123
docker compose start streaming-raw
```

The restoration script prefers a byte-for-byte legacy copy whose digest matches the committed manifest. Its Kafka fallback is not approved for modern batches because raw archive IDs and speed-stream batch IDs use different namespaces.

For a detected live-to-raw event-time gap, run:

```powershell
docker compose stop streaming-raw
docker compose run --rm --no-deps api python -m scripts.repair_archive_gap --include-boundary
docker compose start streaming-raw
```

The gap repair snapshots retained Kafka offsets and checks serving identities before committing a recovery batch. Current raw commits do not yet enforce per-partition offset continuity, so compare source offset ranges before accepting a repair.

## Run performance measurements

Benchmark only a disposable dataset because benchmark events enter the immutable archive:

```powershell
docker compose exec -T api python -m scripts.benchmark --eps 100 --duration 10 --disposable-dataset
```

The saved 10, 100, and 500 events-per-second smoke results are in `output/evidence/performance-benchmark.json`. Every published event was accepted in those short runs. The measurements include the five-second streaming trigger and do not represent sustained capacity.

## Review current limitations

The repository intentionally defers production infrastructure:

- Single Kafka broker, local Spark workers, single-node MinIO, and one PostgreSQL instance
- Local credentials, no Transport Layer Security (TLS), no API authentication, and no external alert routing
- Shared PostgreSQL owner credentials across application services
- MinIO root credentials used by application services
- Fixed local Airflow signing key and predictable demonstration password
- No schema registry, distributed tracing backend, replicated storage, or disaster-recovery automation

The following correctness limitations remain visible and documented:

- Modern Kafka-based archive restoration needs source-offset reconstruction instead of speed-stream batch identities
- Raw commits record Kafka ranges but do not yet reject an internal per-partition offset discontinuity
- A conflict known only through the database ledger can invalidate vehicle profit without retracting the corresponding daily zone aggregate
- SHA-256 publication health protects JSON reports; CSV, HTML, and Parquet exports do not have independent stored digests

The platform does not claim end-to-end exactly-once delivery. Kafka checkpoints, persistent identities, database transactions, archive manifests, and deterministic restatement provide replay safety within the documented single-writer dataset contract.

## Find the submission material

Use these files for assessment and presentation:

- [Final report PDF](output/pdf/fleet-lambda-platform-report.pdf)
- [Submission ZIP](output/fleet-lambda-platform-submission.zip)
- [Implementation status](docs/IMPLEMENTATION_STATUS.md)
- [Final delivered scope](docs/FINAL_SCOPE.md)
- [Demo runbook](docs/DEMO_RUNBOOK.md)
- [Viva questions and answers](docs/VIVA_QA.md)
- [Individual contribution statement](docs/CONTRIBUTION_STATEMENT.md)
- [Architecture decision records](docs/adr/)
- [Clean-install evidence](output/evidence/clean-install.json)
- [Final verification evidence](output/evidence/final-verification.json)
- [Performance evidence](output/evidence/performance-benchmark.json)

## Stop the platform

Preserve data while stopping containers:

```powershell
docker compose down
```

Deleting named volumes permanently removes the demonstration dataset. Do not add `--volumes` unless you intend to rebuild from empty storage.

## Author

Udara Subodhitha Senevirathna, BSc Computer Engineering, University of Ruhuna
