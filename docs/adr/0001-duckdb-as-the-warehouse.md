# 1. DuckDB as the warehouse

**Status:** accepted

## Context

I needed SQL transforms with tests and somewhere for the dashboard to read from, all running free on a laptop and in CI.

## Decision

DuckDB, as one file (`data/warehouse/lakehouse.duckdb`), with dbt-duckdb. It reads JSON, CSV and Parquet directly, so there's no load step. Postgres was the alternative, but it needs a server for what's a single-user workload.

## Consequences

- No server, and the same setup works on the laptop, in CI and in tests.
- One writer at a time, so the dashboard opens it read-only and says "busy" during a build.
- Not built for sharing. Many users or writers would need object storage and a query engine (see the README's limitations).
