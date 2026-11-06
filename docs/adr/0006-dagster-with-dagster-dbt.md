# 6. Dagster with dagster-dbt for orchestration

**Status:** accepted

## Context

The pipeline needs a daily schedule, retries, a record of past runs, and a view of what depends on what. dbt alone runs transforms but not ingestion, and cron gives no lineage or run history.

## Decision

Use Dagster. Ingestion is one asset (`raw/weather_forecasts`), and `dagster-dbt` turns each dbt model into its own asset and each dbt test into an asset check. The ingestion asset fails if any city fails, so dbt never builds on a partial day, and Dagster retries the step twice. The daily schedule (06:00 UTC) ships switched off.

## Consequences

- One UI shows lineage from API to marts, test results per model, and run history.
- `dagster dev` must be running for the schedule to fire; unattended runs need Dagster deployed as a service or a scheduled CI job.
- Adds Dagster and its dependencies (including a protobuf pin to stay compatible with dbt).
