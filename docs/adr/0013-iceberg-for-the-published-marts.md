# 13. Apache Iceberg for the published marts, not the raw layer

**Status:** accepted

## Context

The plan called for Iceberg starting at the raw layer. But raw crypto data is already Parquet that dbt reads day by day, and dbt-duckdb can only read Iceberg by loading whole tables, which would lose that.

## Decision

After each dbt build, publish the four daily crypto marts to Iceberg (PyIceberg, local SQLite catalog in `data/iceberg/`). Raw stays Parquet.

- Tables are partitioned by day and published whole (they're small).
- A publish with identical rows is skipped, so every snapshot is a real change.
- New columns are added to the table (schema evolution).
- Snapshots older than 90 days are expired.

## Consequences

- Time travel answers the useful question: what did reconciliation look like last Tuesday, before later runs filled gaps?
- A test checks DuckDB reads exactly the same rows as PyIceberg. Moving to S3 and Glue later changes the catalog, not the tables.
- `fct_trades` (one row per trade) isn't published; it stays in DuckDB.
