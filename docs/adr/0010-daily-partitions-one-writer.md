# 10. Daily partitions, single-run backfills, one run at a time

**Status:** accepted

## Context

Unlike forecasts, trade history can be fetched for any past day, so crypto data can be backfilled. DuckDB allows one writer at a time. In testing, four runs launched together failed: their processes contended for dbt's package lock, and one exchange download died silently.

## Decision

- Crypto assets use UTC daily partitions starting at `CRYPTO_START_DATE`; the newest is always yesterday, the last complete day.
- A backfill over many days runs as one run (`BackfillPolicy.single_run`); dbt gets the range as `crypto_start_date`/`crypto_end_date`.
- `fct_trades` and `fct_reconciliation_daily` are incremental: a run deletes and re-inserts whole days, so reruns are idempotent and other days are untouched.
- `dagster.yaml` limits the instance to one run at a time, and dbt steps share a one-slot pool.
- A sensor re-runs a day when Project 1 reports a new gap on it.

## Consequences

- Rerunning or backfilling any day is safe and cheap (about 2–3 minutes a day, mostly the polite download).
- Runs queue instead of running in parallel; fine at one day per day, slower for a large backfill.
- Tested: a partition rebuild leaves other days' rows untouched, and running a day twice leaves every crypto mart's checksum unchanged.
