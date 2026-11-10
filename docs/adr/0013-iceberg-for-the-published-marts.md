# 13. Apache Iceberg for the published marts, not the raw layer

**Status:** accepted

## Context

The portfolio plan calls for Apache Iceberg, starting with the raw (bronze) layer, so that any engine can read trusted data and tables can be queried "as of" an earlier time. The crypto raw layer is already open Parquet, partitioned by UTC day, and dbt reads it with DuckDB, skipping the day folders a run doesn't need. dbt-duckdb can only read Iceberg by loading whole tables into memory, which would give that up.

## Decision

Publish the four crypto marts that people and other engines read (`fct_reconciliation_daily`, `fct_ohlcv_daily`, `fct_anomalies_daily`, `fct_trade_gaps`) to Iceberg tables after each dbt build, in a local catalog (PyIceberg with SQLite, under `data/iceberg/`). Keep the raw layer as Parquet and `fct_trades` in DuckDB.

- Each table is partitioned by `trade_date` and published whole (they're small daily summaries).
- A publish whose rows are identical to the current snapshot is skipped (a SHA-256 of the rows is stored on each snapshot), so every snapshot is a real change.
- New columns in a mart are added to the Iceberg table (schema evolution); time travel reads each snapshot with the schema it had then.
- Snapshots older than 90 days are expired after each publish.

## Consequences

- Time travel answers the question that matters: how the reconciliation looked last Tuesday, before later runs filled gaps. Raw files never change once written, so snapshots of them would show little.
- Another engine reads the same tables: a test checks DuckDB's Iceberg reader returns exactly the rows PyIceberg does, for all four tables. Moving to S3 and Glue later means changing the catalog, not the tables.
- The raw layer isn't in Iceberg, which differs from the plan. It can follow with the AWS step if there's a reason to beyond time travel.
- `fct_trades` (one row per trade) isn't published; at this size consumers want the daily tables, and it stays queryable in DuckDB.
