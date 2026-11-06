# 5. Every dbt model is a table

**Status:** accepted

## Context

Staging reads the raw files by a path relative to `dbt_project/`. As views, the staging models stored that relative path, so querying them from anywhere else (the dashboard, a notebook) failed.

## Decision

Materialize every model as a table, including staging and intermediate. The DuckDB file is then self-contained, and the dashboard can read it from any folder.

## Consequences

- A full rebuild takes about 7 seconds at today's size.
- Every run rebuilds everything. At 10x the data, the hourly models should become incremental (merge on city and hour).
- The marts also carry enforced dbt contracts, so their shape can't drift unnoticed.
