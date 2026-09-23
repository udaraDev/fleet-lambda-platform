# Full project review and remaining work

> **Historical audit snapshot (21 September 2026).** This file records issues found before the final remediation. It is not the current project status. Use `FINAL_SCOPE.md` and `output/evidence/final-verification.json` for the delivered state.

Reviewed: 2026-09-21. Scope: assignment PDF (all five pages), PROJECT_PLAN.md,
application code, configuration, tests, documentation and read-only runtime checks.
This review does not change the implementation or inject failures into the live stack.

## Verdict

The project is a working first implementation of a Lambda-style fleet platform,
not yet a submission-ready project and not the full system described in the plan.
The core required stack and both ingestion paths exist. Prioritize correctness,
clear business output, observability evidence and the assessed report before
adding more services. A percentage-complete estimate would obscure the different
importance of these items.

## Verified evidence

- All 24 automated tests passed again during this review.
- The host-side smoke check passed: healthy live ingestion, fleet and zone metrics,
  and the 2026-03-01 report with 12 unique vehicle rows and valid profit arithmetic.
- Kafka, PostgreSQL and API containers were healthy; both producers, Spark and
  Airflow services were running. Airflow reported no DAG import errors.
- Docker Compose configuration validation passed.
- The previous implementation session verified ten isolated PostgreSQL/Parquet
  reconciliation checks and the live 120-second no-data/recovery test. Those
  mutating integration/fault checks were not rerun during this read-only review.
- Additional non-mutating probes reproduced historical-date starvation, silent
  missing-archive handling, and acceptance of unknown/future stream events.
- The live database reported all 12 vehicles as complete for 2026-03-10, with zero
  trips and total profit -5,940,000 cents (-LKR 59,400). The committed archive had
  zero telemetry rows for that date. This is a telemetry-gap result, not evidence
  that every vehicle genuinely did no work.

Passing normal-path tests does not establish correctness for the failure cases below.

## Assignment compliance

### Implemented

- Python continuous telemetry source and daily expense-file source.
- Kafka topic with three partitions, keyed by vehicle.
- Spark Structured Streaming for ingestion, validation and enrichment.
- Airflow-managed reconciliation with retries.
- Meaningful processing: completion-based trip counting, deduplication of trips,
  joining expenses, integer-money profitability and explicit missing expenses.
- Queryable PostgreSQL storage plus raw Parquet and a scheduled JSON report.
- API endpoints for fleet/zone metrics, time-of-day earnings, idle alerts and reports.
- Structured application logging and a basic no-recent-ingestion health rule.
- README, Compose packaging, comments and automated tests.

### Incomplete or needing stronger evidence

- Consolidated business presentation: JSON profitability and separate APIs exist,
  but there is no single readable view of live utilization, area/time-of-day
  earnings and daily losses. Airflow's UI is an orchestration console, not the
  business dashboard. A dashboard is not mandatory if a suitable report is used.
- Financial completeness and bad-input recovery need the fixes below.
- Observability meets the basic health-rule requirement, but end-to-end tracing
  and batch-failure visibility are weak relative to the marking rubric.
- Final report PDF, architecture diagrams and result screenshots are absent from
  the tracked repository. The brief recommends 8-15 pages.
- A 5-10 minute demo video OR a prepared live demonstration remains to be delivered.
- Group contribution statement is needed only if submitting as a group; individual
  submission is permitted. Replace the plan's placeholder member claims with facts.
- Clean-environment reproducibility has not been demonstrated on a second setup.

The brief does not mandate Grafana, Prometheus, MinIO, Spark batch processing,
multiple Airflow DAGs, a dead-letter Kafka topic, or one-minute event-time windows.
Those are plan commitments or optional engineering choices, not separate compulsory
technologies. Keep or explicitly revise them; do not claim them as implemented.

## Findings, in priority order

### F1 - High: missing telemetry can become a complete financial result

Evidence: batch/reconcile.py:37-64 checks only whether ANY committed event has
crossed the next-day boundary. common/domain.py:97-126 treats absent trip totals
as zero and labels a row complete whenever costs exist. The March 10 runtime
result above demonstrates the consequence after simulation downtime.

Required action: track per-day source coverage/gaps separately from expense
availability. Mark results provisional or incomplete when expected telemetry is
missing, and do not present the loss as authoritative. Retain genuine cost-only
vehicles where source coverage is known. Define and document whether the demo
clock pauses, backfills or explicitly records downtime.

Acceptance: a deliberately missing telemetry interval is visible in the API/report;
a genuinely parked vehicle still has valid zero revenue and nonzero costs.

### F2 - High: one bad historical file blocks every later report

Evidence: batch/reconcile.py:99-100 processes all dates in order without isolating
exceptions. A read-only mock of two dates with the first failing attempted only
the first date. Every scheduled retry starts there again.

Required action: schedule dates independently, or continue collecting per-date
failures and fail the overall run after processing other eligible dates. Preserve
the bad date's quarantine and last good output. Avoid recomputing unchanged history
every minute; use input versions/manifests or explicit targeted backfills.

Acceptance: one invalid old file fails visibly while a later valid day is published.

### F3 - High: a conflicting completion can stop the whole stream indefinitely

Evidence: streaming/job.py:84-88 raises inside the database transaction when the
same trip ID has a different vehicle, fare or completion time. The query fails
before committing its batch; restarting retries the same Kafka input. There is
no operator resolution path. This is code-path analysis, not a live fault test.

Required action: implement an explicit quarantine/conflict policy or a documented,
auditable recovery workflow. Do not silently accept conflicting revenue. Handle
conflicting copies of the same event ID BEFORE dropDuplicates at line 56, which
otherwise picks a copy while raw Parquet retains both and batch may later fail.

Acceptance: injected conflicts are visible, accepted records continue or can be
resumed safely, and streaming/batch financial treatment stays consistent.

### F4 - High: committed archive loss is silently interpreted as no events

Evidence: batch/reconcile.py:15-23 uses glob; a missing path yields an empty
iterator. A read-only probe confirmed this. There is no per-file/per-date manifest
or row-count verification. An absent partition can legitimately mean no records,
so merely requiring every date directory in every batch would be incorrect.

Required action: record expected archive files/partitions and counts when committing
a batch, validate them before reconciliation, and refuse to replace good output
when expected input is missing. Differentiate legitimate empty input from lost data.

Acceptance: removing a fixture file listed in a committed manifest fails safely;
a legitimate batch with no records for the requested date still works.

### F5 - Medium: invalid fleet/time context is accepted

Evidence: common/domain.py:40-65 checks timestamp syntax and nonempty vehicle ID,
but not membership or plausible simulated time. A NOT-IN-FLEET event dated 2099
passed streaming.event_error. The API anchors recent metrics to max(event_ts),
so a far-future event can make ordinary vehicles appear stale; the same timestamp
can also open batch-readiness gates prematurely.

Required action: validate registry membership and time bounds against the explicit
simulation clock, with a documented late-arrival policy. Do not compare simulated
event time directly with real wall-clock ingest time as if they used one clock.

Acceptance: unknown vehicles and implausible future events are quarantined while
legitimate late events follow the documented policy.

### F6 - Medium: pipeline health can hide a broken batch path

Evidence: api/main.py:26-35 reads only ingestion commit age and stream counts.
An expense producer outage, failed reconciliation or stale daily report does not
affect this status. Missing expense files also generate no per-date failure: the
batch scanner discovers only files that exist.

Required action: expose separate stream freshness, expected expense arrival,
batch run status, report freshness and DQ counts; add missing-file/failure rules.
Keep liveness distinct from readiness. The existing rule still satisfies the
brief's minimum one-health-rule requirement.

Acceptance: recent streaming plus a failed/stale batch is shown as degraded,
with a useful reason and affected date.

### F7 - Medium: correlation exists in data, not across operational logs

Evidence: producer logs tick/count, streaming logs batch ID/count, and reconciliation
logs run ID/date. trace_id is stored on events and serving rows but is not logged
through those stages. The plan's promised trace-ID walkthrough is not possible
from the current structured logs alone.

Required action: add consistent dataset/batch/run identifiers, sampled event trace
links and structured exception context. Capture one demonstrable end-to-end trace.
No particular logging library is required.

### F8 - Medium: output versions can disagree after an export failure

Evidence: batch/reconcile.py:65-80 commits the database before writing/replacing
the JSON file. Disk/export failure can expose a new database result and an older
JSON report simultaneously. This limitation is already honestly documented.

Required action: use versioned output with an explicit published manifest/status
or an outbox-style export retry; expose the published version in both outputs.
Do not claim a single transaction across PostgreSQL and the file system.

Acceptance: an export-failure test makes the mismatch explicit and retry converges
both outputs to the same published run without duplicate financial rows.

## Remaining work from the full plan

These are incomplete plan features. They need either implementation or an explicit
scope decision, not automatic addition simply because they appear in the plan.

- **Streaming analytics:** actual event-time windows/watermarks and cross-batch
  event-ID semantics; durable idle-alert history; optional GPS-to-zone enrichment.
  Current live totals are SQL lookbacks and idle state uses ordered observations.
- **Clock/window design:** with the current scale a 2-real-second tick advances
  9.6 simulated minutes. Do not add planned 1-minute windows and 2-minute watermarks
  without deciding units, event density and late-arrival behavior.
- **Batch processing:** Spark batch aggregation if required by the retained plan;
  current PyArrow/Python processing is valid at demo scale but keeps trip state
  in driver memory. Date-specific task visibility would improve the single-task DAG.
- **Storage:** MinIO, independent raw/speed consumers, dt/hour organization and
  manifests if retaining that architecture. Current local raw storage and a single
  streaming query are simpler and can be defended for a laptop demonstration.
- **Serving:** consolidated readable report/dashboard; optional CSV/HTML export,
  daily zone summary, unprofitable-only endpoint and configurable zone lookbacks.
  Profit ranking already exists in the daily API's sort order.
- **Monitoring:** Prometheus scraping, Grafana panels/notifications, producer counts,
  Kafka lag, batch duration/DQ metrics and meaningful real-time latency measurements.
  Metrics export alone is not a running monitoring/notification system.
- **Scenarios/tests:** repeatable late, duplicate, malformed, conflicting, missing-file
  and outage scenarios; actual Spark/Kafka integration tests; Airflow failure-path
  evidence; input-to-archive/serving count reconciliation and SQL/API comparisons.
- **Scale/reproducibility:** measured workload/latency results, CI with all test
  dependencies (no silently skipped suites), a clean-clone run and startup resource
  measurements. No fresh data-volume deletion is needed to review the current stack.
- **Demo security:** separate a read-only API database role from the shared fleet
  administration account. Keep demo credentials/ports local; external exposure would
  require a separate security hardening review. No vulnerability scan was performed.

## Documentation corrections

PROJECT_PLAN.md is still a target, not an accurate description of the current build.
Update its completion checklist or add a clearly approved revised-scope section:

- Correct the old SQLite/LocalExecutor proposal to the implemented PostgreSQL metadata.
- Do not claim two independent consumers, MinIO, Spark batch, stateful watermarks,
  eight Airflow stages, Grafana or Prometheus scraping as present.
- Record the intentional trip_completed and integer-cent schema improvements.
- Replace pytest/make-demo assumptions with the actual unittest and integration
  commands; make demo currently runs only an offline accounting sample.
- Refine the Lambda/Kappa justification: explain this implementation's trade-offs
  rather than presenting plan assertions about one engine or full replay as proof.
- Separate an identical business result from identical report bytes: generated JSON
  has a new run_id on each rerun even if financial values are unchanged.
- The brief allows live demonstration instead of video, and individual submission
  instead of a three-person team. Contribution prose must reflect actual work.

## Prioritized completion checklist

### Phase 1 - Correctness and safe recovery

- [ ] Implement F1 and F4: source coverage, archive validation and honest completeness.
- [ ] Implement F2: isolate report dates and support targeted corrected-input reruns.
- [ ] Implement F3 and F5: conflict recovery and fleet/time validation.
- [ ] Add regression tests for each fix without altering live demonstration data.

### Phase 2 - A defensible end-to-end demonstration

- [ ] Create one readable output showing live fleet/zone/time-of-day metrics,
      last update, pipeline/report status, selected-day costs and ranked losses.
      Show LKR amounts, simulated date and provisional/missing-data indicators.
- [ ] Implement F6/F7 and resolve or visibly label F8; capture a failure/recovery trace.
- [ ] Demonstrate duplicate handling, corrected costs, invalid costs and an outage.
- [ ] Check every endpoint against deterministic expected data/direct SQL.
- [ ] Prove clean-environment startup, document resource needs and collect screenshots.

### Phase 3 - Submission package

- [ ] Finalize the scope and make diagrams match what is actually running.
- [ ] Write the report PDF: business requirements; Lambda/Kappa decision across
      latency/replay/consistency/cost; justified stack; diagrams; observability;
      results/screenshots; limitations and production-scale changes.
- [ ] Prepare the 5-10 minute video OR a reliable rehearsed live demo.
- [ ] Prepare the repository link/ZIP, assumptions and group contributions if applicable.
- [ ] Be ready to explain every core processing rule and architectural decision.

### Phase 4 - Optional enhancements after submission-critical work

- [ ] Retained plan extensions: MinIO, separate consumers, Spark batch, formal
      windows/watermarks, Grafana/Prometheus and a broader performance study.

Recommendation: finish Phases 1-3 before expanding the stack. A compact, correct,
well-evidenced solution is closer to the assignment than an unfinished larger plan.

## Review limits

No implementation fixes, production-data changes, service restarts, live fault
injections, fresh-environment builds or performance/load tests were performed in
this review. Only this review document was added. Live metrics are snapshots, not
permanent evidence of completeness. Source-analysis findings are labeled where
they were not injected into the running system. The original guidelines remain
the authority for submission requirements; the project plan adds optional scope.
