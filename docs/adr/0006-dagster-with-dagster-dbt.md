# 6. Dagster with dagster-dbt

**Status:** accepted

## Context

I needed schedules, retries, run history and lineage. dbt doesn't run ingestion, and cron has no lineage or history.

## Decision

Dagster, with each ingestion step as an asset, each dbt model as an asset and each dbt test as an asset check (via `dagster-dbt`). A failed fetch stops the run before dbt, and steps retry. Schedules ship switched off.

## Consequences

- One UI shows lineage from API to marts, test results and run history.
- `dagster dev` has to be running for schedules to fire; unattended runs need a server.
- Adds Dagster's dependencies, including a protobuf pin to stay compatible with dbt.
