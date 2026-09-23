# Viva preparation: 10 likely questions and honest answers

## 1. Why did you choose Lambda instead of Kappa?

The operational and financial views have different truth requirements. Spark
streaming provides low-latency indicative state, while corrected daily expense files
can restate finance results by replaying immutable Parquet. Kappa would be reasonable
if both inputs were versioned events, but expense corrections would require more
complex historical state and retraction handling.

## 2. Is the pipeline exactly once?

No end-to-end exactly-once claim is made. Kafka checkpoints give replayable source
positions; persistent event fingerprints make retries idempotent; conflicting reuse
of an identity is quarantined; raw batches have committed manifests; and daily output
is replaced transactionally by date. A database commit and an external file/object
write cannot be one atomic transaction here, so explicit pending/published status and
retry repair are used.

## 3. How are duplicates different from conflicts?

The same `event_id` with the same canonical business fields is a harmless retry and
is ignored. The same ID with different business content is a conflict. A conflict is
recorded, the implicated live trip/state is removed, and daily profit becomes unknown
instead of choosing one value silently.

## 4. Why use event-time windows and a watermark?

Metrics must reflect when an event happened, not when Spark happened to receive it.
One-minute event-time windows aggregate each zone; the two-minute watermark bounds
how long Spark retains late-window state. Valid late records are still kept in the
raw archive so authoritative batch reconciliation can replay them.

## 5. Why can profit be null?

Zero is a business fact; null means the evidence is insufficient. If telemetry is
incomplete, an event conflicts, or an expense row is missing, the system cannot
honestly classify profit or loss. A completely observed parked vehicle can have zero
revenue and real costs, producing a genuine negative profit.

## 6. How is a daily report restated safely?

Airflow discovers closed dates and takes one committed batch snapshot. The batch job
verifies each archive manifest, validates expenses, uses Spark DataFrames to compute
trips and coverage, then replaces that date's database rows in one transaction.
Input fingerprints and an algorithm version skip unchanged work. A changed expense
file or repaired export causes a new versioned run.

## 7. What happens when a report file is missing or corrupt?

PostgreSQL stores the published run and SHA-256. Health checks compare the file with
that metadata. A missing or changed file marks report health degraded; the next batch
run regenerates the export from the authoritative inputs and republishes its digest.

## 8. What does observability cover?

Structured logs carry stage, batch/run identifiers and row counts. FastAPI exposes
liveness, ingestion freshness, report integrity and Prometheus metrics. Prometheus
evaluates three alert rules, while Grafana provides the pipeline/business dashboard.
Stopping the producer is expected to produce HTTP 503 after the configured freshness
threshold and to recover after valid ingestion resumes.

## 9. What is the main scalability limitation?

This is a single-host classroom system: one Kafka broker, local-mode Spark, single
PostgreSQL and MinIO instances, and small immutable Parquet files. Production would
use replicated services, distributed Spark, partition staging/set-based merges,
Iceberg or Delta compaction, managed secrets/TLS, and capacity tests using realistic
arrival distributions.

## 10. How do you know the speed and batch logic agree?

They share a fixed event contract and UTC timestamp canonicalization. Automated tests
exercise duplicates, conflicts, completeness, cost corrections and publication
repair. A dedicated parity test writes a fixture Parquet day, runs the Spark batch
aggregate and compares vehicle-level trips, revenue, profit, loss flag and status with
the Python reference calculation.
