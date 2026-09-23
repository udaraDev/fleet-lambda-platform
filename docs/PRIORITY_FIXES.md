# Priority fixes - 2026-09-22

## Implemented and verified

- **Financial completeness:** Spark checks each registered vehicle's daily telemetry
  coverage against the configured simulator cadence. Missing/partial coverage yields
  `incomplete_telemetry`, with null profit, margin and loss classification. A parked
  vehicle with complete coverage retains genuine zero revenue and running costs.
- **Independent dates:** a bad/missing expense file no longer blocks other dates.
  The task processes newest dates first, records failures and raises a summary at
  the end. Unchanged verified inputs skip expensive Spark recomputation.
  Five changed dates per scheduled run limits historical backfills; subsequent
  runs continue older dates while prioritizing newly closed days.
- **Archive integrity:** each committed batch stores a SHA-256/row-count manifest.
  Missing or changed files fail reconciliation before replacing good output.
- **Bulk serving:** bounded micro-batches use bulk PostgreSQL lookups and writes,
  not queries per event. Ordered vehicle transitions, trip writes and the ingestion
  ledger commit in one transaction. The cap is 2,500 events, with Kafka configured
  for 2,000 offsets per trigger. This is NOT an unbounded distributed database sink.
- **Replay/conflicts:** persistent event identities cover cross-batch retries.
  Conflicting identities/trips are quarantined and implicated trip revenue is removed
  from the indicative live totals. Affected daily profitability is unknown. Later
  valid records can continue. Quarantine resolution remains a deliberate operator
  action; conflicting amounts are never silently selected as authoritative.
- **Spark batch:** trip conflict detection, aggregation, coverage and cost joins
  operate on Spark DataFrames. Only fleet-size summaries reach Python. Typed archive
  timestamps avoid repeated parsing; legacy fixtures have a timestamp fallback.
- **Native stream validation:** Spark expressions replace the Python validation UDF,
  retaining rejection reasons and quarantine. Unknown fleet IDs and timestamps
  outside the simulation bounds are rejected. The raw archive is not watermark-filtered.
- **Publication status:** database results expose run ID, coverage and pending/published
  export state. File-export failure leaves an explicit pending status; retry publishes
  a matching file. Database and filesystem publication are not claimed to be atomic.
- **Report health:** `/health/reports` detects failed/stalled dates, pending exports
  and report-date lag. `/health/pipeline` continues to check live ingestion freshness.

## Verification

- 34 local unit/API/archive tests.
- Isolated Spark/PostgreSQL integration suite: readiness, duplicate completion
  replay, cost-only vehicles, identical reruns, corrected costs, bad-data quarantine,
  last-good-output preservation, registered-vehicle visibility, missing costs,
  same-date lock, missing archive, bulk cross-batch replay, conflict recovery,
  missing/partial telemetry, native validation, commit retry, Spark conflict handling
  and export-failure recovery.
- Live upgrade baselined 3,600 existing committed batches and 108,312 event identities.
- Upgraded stream resumed the existing checkpoint and committed new batches.
- The previously misleading 2026-03-10 report was restated: all 12 vehicles now show
  incomplete telemetry and null profit, rather than a claimed LKR 59,400 loss.

The database backup is outside the repository at
`C:\8 sem\Big data mini project\tmp\fleet-before-integrity-20260922.dump`.
No source archive, checkpoint or Docker data volume was deleted.

## Upgrade notes

Use the README's existing-dataset migration procedure. Fresh PostgreSQL volumes
execute both schema files automatically. Migration is additive and repeatable;
it baselines legacy files against committed row counts. It cannot prove files were
unaltered before their first checksum baseline. Keep the simulation cadence/fleet
configuration unchanged for a dataset's lifetime.

The simulator's wall clock still advances during downtime. The fix is to report
missing observations honestly, not to fabricate historical telemetry. Historical
reports are restated gradually; a report without publication metadata is legacy and
has not yet been checked under the new coverage rules.

## Historical remaining audit work (completed or superseded)

The following was the backlog on 22 September 2026. Formal windows, separate
consumers, MinIO, Prometheus/Grafana, diagrams and submission evidence were
subsequently implemented. The scaling and production-operations items remain
deliberate limitations in `FINAL_SCOPE.md`, not unfinished assessment requirements.

- Formal event-time window aggregates and watermark policy; current speed metrics
  remain SQL lookbacks. Choose units deliberately: each 2-real-second source tick
  advances 9.6 simulated minutes. Preserve all valid late raw data for batch replay.
- Distributed partition staging/merge if workloads need to exceed the bounded
  serving design; benchmark before raising the cap.
- Broader trace correlation and missing-source freshness measured independently
  from stream progress; Prometheus client/export cleanup and monitoring dashboards.
- Production-scale retention/compaction of event identities and small Parquet files.
- Submission dashboard/report, architecture diagrams and demo evidence from the
  project review remain separate deliverables.

These fixes do not claim that every optional feature in PROJECT_PLAN.md is complete.
