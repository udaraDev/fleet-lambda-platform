# Advanced operations

These procedures are for the local classroom deployment. Start with the
[README](../README.md) for setup and the normal verification path.

## Preserve one logical dataset

The named volumes contain PostgreSQL, Kafka, MinIO, Spark checkpoints, Airflow
logs, Prometheus data, Grafana state, and shared expense/report files. To stop
and restart without deleting them:

```powershell
docker compose down
docker compose up -d
```

Do not add `--volumes` unless you intend to erase the demonstration dataset.
Do not delete only one checkpoint, archive prefix, Kafka volume, or database
volume: the components must remain consistent. Keep the fleet size, simulation
start, tick interval, and clock scale unchanged for an existing dataset.
Downtime advances the simulated clock and can create genuine telemetry gaps.

## Inspect a running system

```powershell
docker compose ps -a
docker compose logs -f streaming-raw streaming-speed producer-stream airflow-scheduler
Invoke-RestMethod http://localhost:8001/health/pipeline
Invoke-RestMethod http://localhost:8001/health/reports
```

The `*-init` containers are expected to exit successfully after setup. Report
health can show `processing` while one closed date is being reconciled; it
returns HTTP 503 if publication, quality, archive-lag, or run checks fail.
If Windows cannot reach the forwarded API port, verify from inside Docker:

```powershell
docker compose exec -T api python -m scripts.verify_running --base-url http://127.0.0.1:8000 --wait-seconds 480
```

That check proves the application path, not host-browser connectivity.

## Check failure detection

When pipeline health is green, the no-data exercise stops only the telemetry
producer, waits for the 120-second threshold, checks HTTP 503, restarts the
producer in cleanup, and confirms recovery:

```powershell
python -m scripts.verify_no_data
```

If the terminal is terminated before cleanup, restart ingestion with
`docker compose start producer-stream`.

## Migrate existing volumes

Fresh installations apply all seven ordered fleet migrations automatically.
Before upgrading existing persistent volumes, back up PostgreSQL and run:

```powershell
python -m scripts.bootstrap_env
docker compose build migration airflow-scheduler
docker compose stop airflow-scheduler streaming-raw streaming-speed producer-stream
docker compose run --rm migration python -m scripts.migrate_integrity
docker compose up -d --no-deps streaming-raw streaming-speed producer-stream producer-batch api airflow-scheduler airflow-webserver
```

The migration checks historical archive counts, backfills canonical Kafka
partition high-water marks, and upgrades legacy report manifests. It fails
when required source material is missing.

## Recover a damaged archive

Identify the exact damaged batch or event-time gap first. Stop the raw writer
before recovery. For a legacy batch with a retained local Parquet copy:

```powershell
docker compose stop streaming-raw
docker compose run --rm --no-deps migration python -m scripts.restore_archive_batch 1234567890123
docker compose start streaming-raw
```

The restoration script first requires a byte-for-byte copy matching the
committed digest. Its Kafka fallback checks the raw batch's source offsets,
retention, and row identities, and refuses non-exact recovery snapshots.
For a detected live-to-raw gap:

```powershell
docker compose stop streaming-raw
docker compose run --rm --no-deps migration python -m scripts.repair_archive_gap --include-boundary
docker compose start streaming-raw
```

Gap repair checks retained Kafka offsets and serving identities before
committing. Normal raw commits reject gaps and overlaps against stored
per-partition high-water marks.

## Reconcile and benchmark

Restate one report date when the source archive and cost file are ready:

```powershell
docker compose exec api python -m batch.reconcile --date 2026-03-01
```

Identical inputs produce identical values. A database advisory lock prevents
overlapping runs for the same date.

Benchmark only a disposable dataset: benchmark events enter the immutable
archive.

```powershell
docker compose run --rm --no-deps tools python -m scripts.benchmark --eps 100 --duration 10 --disposable-dataset
```

The saved 10, 100, and 500 events-per-second runs in
`output/evidence/performance-benchmark.json` are short smoke tests, not
sustained-capacity measurements.
