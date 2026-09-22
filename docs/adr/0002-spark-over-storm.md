# ADR 0002: Spark Structured Streaming Over Apache Storm

**Date:** 2026-03-01  
**Status:** Accepted  
**Deciders:** Udara Subodhitha Senevirathna  

---

## Context

Both the speed layer (streaming ingestion) and the batch layer (daily Spark job)
need a distributed processing engine. The shortlisted candidates for the streaming
engine were Apache Spark Structured Streaming and Apache Storm.

The requirements driving the choice:

- R1: Sub-minute live utilisation metrics computed from a stream of vehicle telemetry.
- R3: Daily profitability recomputed from a Parquet archive — same business logic,
  different trigger.
- The project runs on a single laptop; RAM is constrained.

---

## Decision

**Choose Apache Spark Structured Streaming** for the speed layer, and **Apache Spark
batch** for the daily reconciliation job. Both use the same PySpark DataFrame API.

---

## Justification

### One API for both paths

The Lambda architecture has two processing paths. Using Spark for both means the same
DataFrame transformations, the same type system, and the same UDFs can be reused or
directly inspected across the speed and batch layers. Storm has no batch story and no
DataFrame API; using it for streaming would create a hard boundary between the two layers.

### Event-time processing

Spark Structured Streaming has native support for event-time windowing, watermarks, and
late-data handling via the `event_time` column and `withWatermark` API. Storm's at-least-once
tuple processing requires the application to implement its own time-tracking logic.

For this project, a 2-second real-time tick advances 9.6 simulated minutes. Native event-time
processing ensures that the time dimension of metrics is correct relative to simulation time,
not wall-clock arrival time.

### DataFrame API and SQL integration

Spark's DataFrame API allows the batch aggregation (trip collapse, outer join, profit
computation) to be expressed as composable, testable transformations. The same SQL expressions
can run on both the micro-batch stream and the Parquet archive.

### Resource model

Spark in local mode (`local[*]`) runs the driver and executors in one JVM process, which
is compatible with a laptop environment alongside Kafka, PostgreSQL, and Airflow.
Storm requires a separate Nimbus + Supervisor + ZooKeeper process set, which would
significantly increase RAM consumption without adding capability relevant to this use case.

---

## Rejected Alternative: Apache Storm

| Concern | Storm | Spark |
|---|---|---|
| Batch story | None (Trident adds it awkwardly) | Native — same API |
| Event-time windowing | Application-level | Native watermark + window API |
| DataFrame/SQL | No | Yes |
| RAM per node | High (Nimbus + ZK + Supervisor) | Lower in local mode |
| Latency advantage | Yes (~10ms vs ~1s) | Irrelevant at 2-second source tick |

Storm's primary advantage — very low processing latency — is irrelevant here. The
telemetry producer emits one tick every 2 real seconds. A 1-second micro-batch trigger
interval in Spark Structured Streaming provides sufficient freshness for sub-minute
operational metrics (R1/R2). The latency difference does not affect any requirement.

---

## Consequences

### Positive
- Single framework for both pipeline layers reduces cognitive overhead.
- Spark's DataFrame API makes transformations unit-testable in pure Python without
  a cluster (using `domain.py:reconcile()` as the reference implementation).
- The `ALGORITHM_VERSION` field ensures that a Spark version change triggers
  automatic restatement of all published daily reports.

### Negative
- Spark's startup overhead (~10 seconds) means the streaming job takes longer to
  begin processing than a Storm topology would after a restart.
- Spark's local mode does not provide fault isolation between driver and executors;
  a JVM crash stops all processing. Acceptable for a classroom demonstration.
- The micro-batch model means events are processed in ~5-second windows, not
  continuously. Burst events within a batch are atomic from the serving layer's
  perspective but do not update the live view mid-batch.
