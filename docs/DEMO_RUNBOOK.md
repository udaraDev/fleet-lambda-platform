# Prepared live demo (approximately 8 minutes)

Start Docker and the Compose stack in advance. Open http://localhost:8001 and
http://localhost:8080. Make sure a complete date is available before presenting;
startup/downtime dates can correctly show incomplete telemetry. Do not delete live
state to make results look better. Use the isolated integration tests for faults.

## 0:00-1:00 - Business question and decision

Explain utilization/earnings now versus profitability after daily fuel and maintenance
costs. Show the architecture diagram in the PDF. Explain why Lambda fits revised cost
files and how Kappa would represent corrections. State the limits of a single-node demo.

## 1:00-2:00 - Two sources and simulated clock

Show producer logs and the expense file arrival. One day = five real minutes; a two-
second tick = 9.6 simulated minutes. Fares are counted once on trip completion, not on
every GPS event. Every fourth vehicle is deliberately parked.

## 2:00-3:00 - Live business view

Show fleet activity by zone and select a complete report date. Explain cents versus
LKR display and why live lookbacks are indicative, not finalized tumbling windows.
Point out active = enroute/on_trip and idle ratio = idle/reporting vehicles.

## 3:00-4:00 - Daily orchestration and results

Show Airflow's daily_profitability DAG and successful task logs. Show report run ID,
algorithm version and per-vehicle quality. Explain null profit for incomplete telemetry
or missing expenses, compared with genuine losses for observed parked vehicles.

## 4:00-6:30 - Observable failure and recovery

With a healthy stack run `python -m scripts.verify_no_data`. It stops the producer,
waits for HTTP 503 after 120 seconds, restarts it in a cleanup handler and verifies
HTTP 200. During the wait explain archive manifests, event identities and checkpoint
recovery. This intentional outage leaves a real telemetry gap; that is expected.
If the terminal is forcibly killed, run `docker compose start producer-stream`.

## 6:30-7:30 - Correctness evidence

Show saved integration evidence: 23 checks covering replay, malformed inputs,
expense corrections, missing/corrupt output repair, quarantine, archive loss,
timestamp parity, conflict state removal and failed-export recovery. The suite uses
an isolated schema and temporary files; it does not corrupt live reports.

## 7:30-8:00 - Limitations and next steps

Explain coupled raw/live writes, bounded driver bulks, no production authentication,
no formal watermark finalization, small-file overhead and honest incomplete history.
Show FINAL_SCOPE.md and explain explicitly deferred items.

## Viva rehearsal

Be ready to explain the completion-date rule; why duplicates differ from conflicts;
why two separate API queries can mislabel a run; the role of archive checksums; why
report publication is not a cross-system atomic transaction; replay after restart;
why no observations cannot establish a zero-income day; and why a dashboard is not
the same thing as an Airflow administration page.

## Submission checklist

- Read the PDF and verify author/course details before submission.
- Rehearse the live demo; students must personally defend their implementation.
- Submit the PDF and generated source ZIP (or an equivalent complete Git repository).
- If a group, add truthful contributions; if a recording is required locally, record
  this walkthrough. A video file has not been fabricated or claimed as recorded.
