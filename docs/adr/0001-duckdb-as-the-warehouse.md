# 1. DuckDB as the warehouse

**Status:** accepted

## Context

The pipeline lands about 13,000 hourly rows a day for 79 cities. It needs SQL transforms with tests, a place for the dashboard to read from, and it has to run on a laptop and in CI for free.

## Decision

Use DuckDB, stored as a single file at `data/warehouse/lakehouse.duckdb`, with dbt-duckdb for the transforms. DuckDB reads the raw JSON and CSV files directly, so there is no separate load step. Postgres was the main alternative, but it needs a running server for what is a single-user analytical workload.

## Consequences

- No server to run, and the same warehouse works on the laptop, in CI and in tests (each test builds its own throwaway file).
- One writer at a time. A dbt build locks the file, so the dashboard opens it read-only and shows "busy, try again" during a build.
- Not shared: remote users or several pipelines writing at once would need object storage, an open table format and a query engine (see the README's "What breaks at 10x").
