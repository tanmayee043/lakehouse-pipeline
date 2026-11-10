# 5. Weather models are tables, crypto staging is inlined

**Status:** accepted

## Context

As views, the weather staging models stored a file path relative to `dbt_project/`, so querying them from the dashboard or a notebook failed.

## Decision

Every weather model is a table, so the DuckDB file works from any folder. Crypto staging and intermediate models are ephemeral instead: dbt inlines them, so nothing stores a path and DuckDB only opens the days a run needs. `fct_trades` and `fct_reconciliation_daily` are incremental, one day at a time.

## Consequences

- Everything the dashboard reads is a table.
- Weather rebuilds fully each run (about 7 seconds). At 10x the data, the hourly models should go incremental too.
- Every mart has an enforced dbt contract, so its shape can't drift unnoticed.
