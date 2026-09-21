# Project review after priority fixes

Reviewed 22 September 2026 (Asia/Colombo). Read-only implementation review against PROJECT_PLAN.md and the five-page EC8203 assessment brief. This supersedes the remaining-task conclusions in PROJECT_REVIEW.md where fixes have since landed. No implementation or live data was changed for this review.

## Verdict

The core pipeline is working and substantially improved. It is not yet the full architecture promised in the plan, and submission assets are unfinished. Finish correctness and reproducibility first; avoid adding infrastructure solely to make the project look larger.

## Evidence and limits

- Fresh local run: 34 unit/API/archive tests passed. A host dependency deprecation warning remains.
- Fresh read-only live smoke test passed: ingestion, fleet metrics, zone metrics, daily output, unique vehicle/day rows and profit arithmetic.
- All eight long-running Compose services were running. PostgreSQL, Kafka and API reported healthy. Airflow reported no DAG import errors.
- At the health snapshot, ingestion age was approximately 2.8 seconds. Report health was degraded: expected simulated date June 29, latest published June 28, zero failed dates, zero stalled dates and zero pending exports. A subsequent database read showed June 29 processing. This is a snapshot of report lag, not proof of a broken scheduler.
- Database contained 120 report dates, none lacking publication metadata; 736 vehicle/day rows were incomplete_telemetry and 704 complete. Historical downtime cannot be repaired by inventing events. Publication metadata alone does not prove every date has been restated under the latest fingerprint version.
- Previous work records a passing isolated Spark/PostgreSQL integration suite. It was not rerun during this review; no live fault injection, load test, clean-machine rebuild, penetration test or backup restoration was performed.

## Earlier audit: what is already resolved

- Per-event database queries were replaced with bulk lookups and writes. Driver collection remains deliberately capped at 2,500 rows, with Kafka configured for 2,000 offsets per trigger. This is a bounded demo sink, not a distributed production sink.
- Production daily reconciliation now uses Spark DataFrames for trip aggregation, coverage and expense joins. The pure-Python reference implementation is not the production batch path.
- Persistent event identities now provide cross-batch replay protection. Missing Spark watermark deduplication does not mean cross-batch deduplication is absent.
- Native Spark expressions replaced the production validation UDF.
- Archive manifests, incomplete-telemetry flags, conflict quarantine, publication state and batch health were added.

## Priority 1: remaining correctness fixes

### 1. Recover missing or corrupted published report files

Evidence: batch/reconcile.py:65-70 checks only the input fingerprint and database export_status before returning unchanged. It does not check the JSON output's existence, content or run ID. Once an already-published file is lost or damaged, unchanged inputs will not regenerate it; report health can still show published.

Task: store/verify output integrity and version, regenerate missing or mismatched exports, and test file deletion/corruption after successful publication without damaging live files.

### 2. Read daily rows and publication metadata from one snapshot

Evidence: api/main.py:132-138 makes two fetch_all calls. common/db.py opens a separate connection/transaction for each. A reconciliation commit between those calls can return old vehicle rows labelled with a new run ID and coverage.

Task: fetch both in one SQL statement or a repeatable-read snapshot. Add a concurrent-publication regression test. Apply consistent-snapshot thinking to other multi-query summaries too.

### 3. Align duplicate identity rules across live and batch processing

Evidence: streaming/sink.py:19-22 normalizes event_ts to UTC before fingerprinting. batch/spark_reconcile.py:53-58 fingerprints the raw event_ts string. For otherwise identical records, `2026-03-01T00:00:00Z` and `2026-03-01T00:00:00+00:00` represent the same instant and are duplicates live, but different signatures in batch. Daily profit can consequently become unknown despite an accepted live replay.

Task: define a shared canonical business-event contract and implement equivalent Spark expressions. Test timezone spelling, numeric representation and schema evolution. This finding follows directly from code paths; the timestamp case was not replayed into the live dataset.

### 4. Invalidate live state when its event is later conflicted

Evidence: streaming/sink.py:80-83 removes implicated completed trips, then can return without touching rt_vehicle_state. A previously accepted event that later becomes conflicted can remain the vehicle's displayed status/zone/idle start until a newer accepted event arrives.

Task: mark affected state unknown or rebuild it from non-conflicted history. Test a conflict against the current state followed by no new telemetry. Provide a documented operator workflow for resolving quarantined identities; never silently select a conflicting fare.

## Priority 2: health, recovery and bounded scaling

- Separate new-source freshness from valid micro-batch commits. api/main.py:28 regards rows_valid > 0 as fresh, including exact replays and conflicts. A duplicate-only replay can keep ingestion healthy without new business observations. Expose accepted/duplicate/conflict counts and source/event lag.
- Check expected report-date coverage, not just the maximum published date (api/main.py:59-65). A missing historical date with no run record can be hidden by a newer successful report. Add a normal reporting grace period and distinguish expected processing from sustained lag.
- Complete and verify historical restatement under the current algorithm version. Make the version visible rather than inferring it from the existence of publication metadata. Explain incomplete historical observations in the demo.
- Enforce a single archive writer or use unique staging with fenced publication. streaming/job.py:60 writes the shared batch directory before the advisory lock at line 73. Database serialization alone does not prevent a second independently started writer from overwriting the same archive. Current single-writer Compose operation reduces this risk; multi-writer safety is not established.
- Profile unchanged-date scans: run_available loads all manifests and verified_paths hashes each day's source files before the unchanged shortcut. The five-changed-date budget does not cap unchanged checks. Historical scanning, small files and repeated Spark actions will become expensive.
- Add retention/compaction planning for raw files, event keys and run history, plus a tested backup/restore procedure that keeps database, archive and checkpoint consistent. Do not delete individual state components as a reset strategy.
- Before any external deployment, replace demo credentials/secrets, use least-privilege database roles (including read-only API access), and add appropriate authentication/TLS. Current host ports are loopback-bound; this is a local teaching deployment, not an internet-ready service.

## Priority 3: reconcile the plan with the delivered architecture

These are commitments in PROJECT_PLAN.md, not separate mandatory technologies in the assessment brief:

- Spark event-time tumbling windows, explicit lateness/watermark policy and rt_zone_metrics are still absent. Current live metrics use PostgreSQL lookbacks. Either implement the promised windows or document and justify the reduced design. A two-real-second tick advances 9.6 simulated minutes, so the proposed one-minute window/two-minute watermark need deliberate units. Keep valid late raw events available for batch reconciliation.
- Airflow currently exposes one validate_reconcile_publish task. Stage-level validation/aggregation/publication tasks would improve visibility and retries, but multiple DAGs are not required.
- MinIO and separate raw/speed consumers are not implemented. Shared local Parquet and one checkpointed stream are legitimate documented simplifications for this demo; they have coupled failure and scaling characteristics.
- Prometheus scraping, Grafana dashboards, complete trace correlation and a metrics client are not implemented. Basic structured logs and health checks already exist. Manual fixed numeric Prometheus output is a maintenance issue, not evidence of an escaping vulnerability here.
- Daily zone summaries, durable alert history and a dedicated unprofitable-vehicle route are additional plan items, not blockers to the existing per-vehicle business report.

## Required submission work

The brief requires a consolidated report OR dashboard; a separate visual dashboard is not mandatory. The generated JSON/API profitability output already provides a consolidated result, but a readable business-facing view would improve the demonstration. Airflow's UI is an orchestration view, not a fleet dashboard.

1. Produce the final PDF report (recommended 8-15 pages): use case, explicit Lambda-versus-Kappa decision, rejected alternative, latency/replay/consistency/cost trade-offs, actual architecture diagrams, technology rationale, observability, screenshots/results and limitations.
2. Capture evidence from the actual running implementation: live fleet/zone metrics, a complete daily report, incomplete-data flags, quarantine, Airflow execution and a triggered health rule. Do not claim planned components are present.
3. Prepare a 5-10 minute video OR rehearsed live demo. Explain time compression, integer cents, indicative live metrics versus reconciled output, and missing-data behavior. Be able to defend the core logic in the viva.
4. Validate clean-clone setup and package all source/config/migrations/tests as a repository link or ZIP. Many priority-fix files remain untracked or modified; a submission based only on the current commit would omit them.
5. Update PROJECT_PLAN.md/README/status documentation to separate delivered design from future work; remove stale SQLite/LocalExecutor wording, obsolete commands and placeholder team claims. Include contribution statements only for a group submission, based on actual work.

## Verification still needed

- Regression tests for the four correctness findings above and missing historical report detection.
- Explicit late/out-of-order, replay, conflict, restart and source-outage demonstrations through Kafka, with count reconciliation against archive and serving results.
- A representative 10/100/500 events-per-second benchmark if retaining that plan commitment; measure latency and resource use before changing the serving cap.
- Reproducible container-based unit/integration checks and preferably CI, avoiding dependence on unpinned host packages.
- Clean-environment setup and coordinated backup restoration tests. The existing offline make demo is not proof of full end-to-end reproducibility.

## Suggested execution order

1. Fix report export recovery, snapshot consistency and canonical identity; cover them with regression tests.
2. Correct conflicted live state and health semantics; verify current-version historical output.
3. Decide which architecture-plan extras to implement and explicitly defer the rest.
4. Run failure/recovery and clean-setup verification; capture screenshots and measured results.
5. Finish PDF, demo and repository/ZIP packaging.

Avoid adding MinIO, a second DAG or a large dashboard before fixing result consistency and completing the required report. These additions do not substitute for the architecture argument or reproducible evidence.
