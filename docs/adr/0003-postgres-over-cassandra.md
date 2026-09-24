# ADR 0003: PostgreSQL Over Apache Cassandra for the Serving Layer

**Date:** 2026-03-01  
**Status:** Accepted  
**Deciders:** Project team (Threemavithana T.M.; Senevirathne P.U.S; Kodikara A.W.)

---

## Context

The serving layer needs to store and query two classes of data:

1. **Live state** (speed path): current vehicle status, recent completed trips,
   idle durations — updated every micro-batch (~5 seconds), queried with
   rolling-lookback aggregates and zone-level GROUP BY.

2. **Daily financial results** (batch path): per-vehicle profitability rows for
   each simulated date — written once per Airflow run, queried by date and
   vehicle with joins and ORDER BY.

Both classes are small in absolute terms: 12 vehicles, O(100) daily rows per date.
The shortlisted candidates were PostgreSQL and Apache Cassandra.

---

## Decision

**Choose PostgreSQL.**

---

## Justification

### Ad-hoc joins and aggregates

The live metrics endpoints (`/metrics/fleet`, `/metrics/zones`) compute rolling
aggregates with GROUP BY and FILTER clauses. The daily report endpoint joins
`daily_vehicle_profit` with `daily_report_status` in a single SQL statement to
guarantee snapshot consistency. Neither of these access patterns is natural in
Cassandra, which is designed for pre-modelled, denormalised, single-partition reads.

Using Cassandra would require materialised views or application-level joins for
every multi-table query, adding complexity without benefit.

### Data volume

This project serves 12 vehicles. Daily vehicle profit rows are O(12 × number of
simulated days). The absolute row counts never justify a distributed wide-column
store. Cassandra's advantages — horizontal write scaling, multi-region replication,
tunable consistency — are irrelevant at this scale.

### Transactional guarantees

The batch path requires atomic upserts: delete the old day's rows and insert the
restated rows inside one transaction. If the process dies mid-write, the old rows
must survive intact. PostgreSQL's ACID transactions provide this directly.
Cassandra's lightweight transactions (using Paxos) are significantly more complex
to reason about and carry a performance penalty that is not warranted here.

### Advisory locks

The batch reconciliation uses `pg_try_advisory_lock` and `pg_advisory_xact_lock`
to serialise writers per simulated date without holding row locks across the Spark
aggregation. This pattern is native to PostgreSQL and has no equivalent in Cassandra.

### FastAPI integration

`psycopg2` is a mature, synchronous PostgreSQL driver. The entire API is
synchronous FastAPI. A Cassandra driver would introduce the `cassandra-driver`
dependency and require different connection lifecycle management without providing
any capability relevant to this use case.

---

## Rejected Alternative: Apache Cassandra

| Concern | Cassandra | PostgreSQL |
|---|---|---|
| Ad-hoc GROUP BY + JOIN | Requires denormalisation or app-level join | Native SQL |
| ACID transactions | Lightweight transactions (Paxos, expensive) | Full ACID |
| Advisory locks | Not supported | Native pg_advisory_lock |
| Volume justification | Horizontal scale | Not needed at 12 vehicles |
| Schema evolution | Additive only without downtime | ALTER TABLE + migrations |
| Operational complexity | Requires tuning RF, consistency levels, compaction | Single-node, no tuning |

Cassandra's advantages become real when write throughput exceeds millions of events
per second across multiple regions, or when the team needs tunable consistency for
globally distributed reads. Neither condition applies here.

---

## Production Scale Note

At real fleet scale (thousands of vehicles, multiple cities, high-frequency GPS),
the serving layer would be redesigned:

- **Live state:** A time-series database (TimescaleDB, InfluxDB) or a Redis
  sorted set for current vehicle positions, with pre-aggregated zone metrics
  published by the streaming job.
- **Daily financials:** An OLAP store (BigQuery, Redshift, Snowflake) for
  historical reporting, with dbt managing the transformation models.
- **PostgreSQL** would remain for transactional data (configuration, audit trail,
  user accounts) rather than high-volume analytics.

The choice of PostgreSQL for this project is appropriate for the demonstrated scale
and is explicitly acknowledged as a single-node classroom implementation in
FINAL_SCOPE.md and the report limitations section.

---

## Consequences

### Positive
- All queries expressible in standard SQL; no Cassandra query modelling overhead.
- ACID transactions make batch upserts and advisory locks straightforward.
- Single dependency (`psycopg2`); no additional drivers or connection pools needed.
- Schema migrations are three additive `.sql` files run automatically by the
  PostgreSQL Docker image on first start.

### Negative
- PostgreSQL is not horizontally scalable for writes without Citus or sharding.
  At the demonstrated scale this is irrelevant.
- No built-in replication in this deployment (single container). Data is stored on
  a named Docker volume; loss of the volume loses all serving data. Mitigated by
  the fact that all financial truth is in the immutable Parquet archive, from which
  serving data can be fully rebuilt by re-running the batch DAG.
