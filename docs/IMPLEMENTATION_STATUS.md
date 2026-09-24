# Implementation status

Current delivered scope is defined in [FINAL_SCOPE.md](FINAL_SCOPE.md). The original
`PROJECT_PLAN.md` remains a proposal and traceability record rather than a checklist
of mandatory optional features.

Implemented:

- two deterministic Python data sources feeding Kafka and daily expense landing;
- independent Spark raw and speed consumers with separate checkpoints;
- immutable MinIO Parquet archives with committed row-count/SHA-256 manifests;
- Kafka-offset identities for raw commits and a fail-closed retained-log gap repair;
- Spark event-time windows with a two-minute watermark and retractable conflict
  corrections backed by per-event metric contributions;
- executor-partition staging plus transactional set-based PostgreSQL live state,
  completed trips, metrics, alerts and daily results;
- PySpark daily reconciliation orchestrated by Airflow, with quarantine and
  publication-integrity checks;
- FastAPI fleet, parameterised zone-window, alert, report and health endpoints;
- structured logs, Prometheus rules and a provisioned Grafana dashboard;
- cross-layer live/raw lag health and incremental archive-integrity verification;
- reproducible tests, clean-install evidence, report, demo runbook and submission
  packaging artifacts.

Operational limitations and intentional production deferrals are documented in
[FINAL_SCOPE.md](FINAL_SCOPE.md).
