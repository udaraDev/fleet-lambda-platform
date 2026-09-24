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

Show the green local suite (54 tests plus 17 subtests), the separately executed
Spark/Python parity test, and saved integration evidence covering replay, malformed
inputs, expense corrections, missing/corrupt output repair, quarantine, archive
loss, timestamp parity, conflict state removal and failed-export recovery. The
integration checks use isolated state and do not corrupt live reports.

## 7:30-8:00 - Limitations and next steps

Explain independent raw/speed consumers, bounded serving batches, no production
authentication, watermark finalization, small-file overhead and honest incomplete
history. Show FINAL_SCOPE.md and explain explicitly deferred production hardening.

## Viva rehearsal

Use `VIVA_QA.md` for ten prepared questions and honest answers. Be ready to explain
the completion-date rule, duplicates versus conflicts, archive checksums, publication
atomicity, replay, null profitability, and the distinction between Grafana's business
dashboard and Airflow's administration UI.

## Submission checklist

- Read the PDF and verify author/course details before submission.
- Rehearse the live demo; students must personally defend their implementation.
- Submit the PDF and generated source ZIP (or an equivalent complete Git repository).
- Confirm the three names, student numbers, and contribution statement before
  presenting. If a recording is required locally, record this walkthrough. A
  video file has not been fabricated or claimed as recorded.
