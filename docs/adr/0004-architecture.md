# 4. Architecture

Date: 2026-09-22

## Status
Accepted

## Context
The project requires a telemetry processing pipeline that provides both real-time operational state (active vehicles, idle ratios, alerting) and immutable daily financial reconciliations. We need to decide on the core data processing paradigm.

## Decision
We will use a Lambda Architecture, consisting of:
- **Speed Layer**: Spark Structured Streaming validates Kafka events, computes tumbling windows with a watermark, and stages executor partitions for a transactional PostgreSQL merge.
- **Raw Layer**: An independent Spark Structured Streaming consumer writes committed Parquet and integrity manifests directly to MinIO.
- **Batch Layer**: Airflow orchestrating PySpark jobs to process daily CSV expense files and reconcile them against immutable Parquet archives on MinIO (S3).
- **Serving Layer**: FastAPI reads live metrics and historical daily results from PostgreSQL; four versioned export formats are published as files.

## Consequences
- **Pros**: Clear separation of concerns; immutable archives allow restating financials if corrected expense files arrive; avoids complex event-time state management for accounting logic inside the streaming layer.
- **Cons**: Requires two distinct processing codebases (Speed and Batch); eventual consistency between live metrics and daily financial results.
