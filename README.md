# Fleet Lambda Platform

End-to-end **Lambda architecture** data pipeline for real-time ride-hailing fleet analytics and daily vehicle profitability reconciliation.

> **Status:** 🚧 In progress. See [`PROJECT_PLAN.md`](PROJECT_PLAN.md) for the full design and roadmap.

## Overview

A ride-hailing operator needs two things:

1. **Live visibility** into fleet utilisation and earnings by zone.
2. **A daily reconciliation** showing which vehicles lose money once fuel and maintenance costs arrive.

**Business question:** *What is fleet utilisation and earnings by zone right now, and which vehicles become unprofitable once yesterday's costs are factored in?*

## Architecture

```
                         ┌──────────────── Speed layer ────────────────┐
Stream simulator ─► Kafka ─► Spark Structured Streaming ─► PostgreSQL (rt_*)  ─┐
  (GPS / trip events)    │                                                     │
                         └─► Raw sink ─► Parquet data lake (MinIO)             ├─► FastAPI ─► Grafana
                                              │                                │
Batch simulator ─► Daily expense CSV ─► Airflow DAG (validate ► join ► profit) ─┘
  (fuel / maintenance)                  └────────── Batch layer ──────────┘
```

- **Speed layer:** low-latency, windowed utilisation metrics and idle-vehicle alerts.
- **Batch layer:** recomputes authoritative per-vehicle profitability from an immutable raw dataset.
- **Serving layer:** PostgreSQL tables exposed through a REST API and dashboards.

**Why Lambda over Kappa?** The daily cost feed is an external, *correcting* dataset that can arrive late or be re-issued. Recomputing a day in batch is simple and deterministic; restating closed windows in a stream-only design is not.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Apache Kafka (KRaft) |
| Stream processing | Apache Spark Structured Streaming |
| Orchestration | Apache Airflow |
| Data lake | Parquet on MinIO (S3-compatible) |
| Serving store | PostgreSQL |
| API | FastAPI |
| Observability | structlog, Prometheus, Grafana |
| Data quality | Great Expectations |
| Deployment | Docker Compose |

## Key Features

- Simulated streaming telemetry and daily-batch cost feeds (Python)
- Event-time windowing, watermarking and deduplication for late or duplicate events
- Data quality gate with row quarantine for bad input files
- Idempotent loads, so re-running a day restates results instead of duplicating them
- Structured logging with trace IDs, pipeline metrics and alert rules

## Simulated Clock

**1 simulated day = 5 real minutes**, so a full daily cycle can be demonstrated in one session.

## Planned Project Structure

```
simulators/     Stream and batch data generators
common/         Shared schemas and transforms (used by both layers)
streaming/      Spark raw sink and speed layer jobs
batch/          Validation, aggregation and profitability logic
airflow/dags/   Daily batch orchestration
api/            FastAPI serving layer
observability/  Prometheus and Grafana configuration
sql/            Serving database schema
tests/          Unit tests
docs/adr/       Architecture decision records
```

## Getting Started

> Setup instructions will be added once the Docker Compose stack is implemented.

```bash
git clone https://github.com/udaraDev/fleet-lambda-platform.git
cd fleet-lambda-platform
cp .env.example .env
docker compose up -d
```

## Author

**Udara Subodhitha Senevirathna**. BSc Computer Engineering, University of Ruhuna
