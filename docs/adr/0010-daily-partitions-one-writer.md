# 10. Daily partitions, one run at a time

**Status:** accepted

## Context

Trade history can be fetched for any past day, so crypto can be backfilled. But DuckDB has one writer, and in testing four parallel runs failed on lock contention.

## Decision

- Crypto assets are partitioned by UTC day; the newest is always yesterday.
- A backfill over many days runs as one run.
- Incremental models replace whole days, so re-runs are safe.
- Only one run at a time, and dbt steps share a one-slot pool.
- A sensor re-runs a day when the Crypto project reports a new gap on it.

## Consequences

- Re-running or backfilling a day is safe and takes 2–4 minutes.
- Runs queue instead of going in parallel, which is fine at one day per day.
