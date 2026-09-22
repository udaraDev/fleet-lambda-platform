# Delivered scope and architecture decision

This document supersedes proposed features and completion claims in the original
PROJECT_PLAN.md, PROJECT_REVIEW.md and historical increment notes.

## Accepted implementation

The delivered project is a local, single-node Lambda demonstration: Python
telemetry -> Kafka -> Spark Structured Streaming -> Parquet and PostgreSQL live
state; Python daily expense CSV -> Airflow -> Spark batch -> PostgreSQL and
versioned daily JSON. FastAPI provides a business results page, API, structured
request logs, ingestion freshness and report-integrity health checks.

One simulated day is 300 real seconds. Event timestamps are UTC; money is integer
LKR cents. Current live metrics are indicative latest-state and rolling lookback
queries. Daily accounting uses complete vehicle-day observations and expense joins.
Unknown, missing and conflicting evidence never becomes a claimed profit/loss.

## Decision: Lambda rather than Kappa

Low-latency operational state and revisable daily accounts have different consistency
needs. Lambda allows rebuilding financial results from immutable committed archives
after a corrected cost file arrives. Kappa could replay both feeds through one
streaming topology, but would require representing expense revisions as events and
managing historical keyed state/retractions for accounting. For a short local project,
separate scheduled reconciliation is easier to explain and verify.

The cost is duplicated identity/business logic, two processing paths and eventual
consistency. Shared event fields, timestamp parity tests, explicit result versions,
and completeness flags mitigate rather than eliminate that cost. Kappa becomes more
attractive if all sources are naturally versioned event streams and a team can operate
their replay/state infrastructure reliably. Lambda is not universally superior.

## Explicitly deferred features

- Formal Spark tumbling windows and watermarks: SQL lookbacks satisfy this demo's
  immediate metrics; no claim of event-time window finalization. A source tick spans
  9.6 simulated minutes, making the original one-minute window inappropriate without
  a revised cadence. All valid late archives remain eligible for batch restatement.
- MinIO and separate raw/speed consumers: local Parquet and one checkpointed writer
  keep laptop operations manageable. Raw/serving availability is coupled.
- Prometheus server, Grafana, push notifications and full distributed tracing:
  existing structured logs, metrics export and HTTP health rules satisfy the basic
  observability deliverable. A functional business view is included independently.
- Multiple DAGs, stage-specific orchestration, durable business-alert history,
  daily zone aggregates and high-volume distributed sink staging: future work.
- Production credentials, authentication, TLS, replication and disaster recovery:
  not an externally deployable service. Keep ports loopback-only.
- 10/100/500 eps benchmark: removed from the submission scope; no unmeasured throughput
  or p95 claim is made. Serving remains capped, not advertised as production scale.

These deferrals are deliberate scope reductions, not completed implementation claims.
The assessment requires meaningful processing, a consolidated report OR dashboard,
and at least one alert/health rule, not these specific optional technologies.

## Operational contracts

- Treat PostgreSQL, Kafka, raw Parquet and Spark checkpoint as one dataset. Use
  coordinated snapshots with writers stopped for any recovery. Never reset one alone.
- One archive writer owns a dataset. A session advisory lock fences file writes;
  a separate transaction lock protects the commit ledger. Independent checkpoints
  must not target the same dataset, even sequentially.
- Keep fleet count and simulator cadence unchanged for the lifetime of the dataset.
- Report algorithm version 4 and an export SHA-256 identify verified publication.
  New data/cost corrections or export damage trigger restatement.
- Rejected identities remain quarantined. Resolution requires audited source
  correction and a deliberately rebuilt dataset, not ad-hoc deletion of conflict rows.
- Source freshness checks newly accepted event identities and simulated-event lag;
  exact duplicates cannot refresh it. Historical migration timestamps alone are not
  trustworthy source arrival times, so event lag is also checked.

## Submission choices

The submission uses a prepared live demo, not an unrecorded video presented as done.
The demo runbook contains timings, commands, expected outcomes and viva questions.
Author identity follows the existing README. If this is a group submission, the
actual member contributions must be supplied by the students; none are invented.
