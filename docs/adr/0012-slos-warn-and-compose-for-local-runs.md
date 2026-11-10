# 12. SLOs warn instead of failing; Docker Compose for local runs

**Status:** accepted

## Context

I wanted written SLOs checked automatically, and a one-command setup. That raised two questions: what happens when an SLO is missed, and how much production setup belongs locally.

## Decision

**SLOs are dbt tests set to warn.** A miss shows in dbt, Dagster and the dashboard, but the pipeline keeps going. The usual causes (a schedule off, the Crypto project offline) aren't fixed by stopping.

**Docker Compose runs one pinned image as two services:** Dagster and the dashboard. Both share `./data` and `./dagster_home`, and the Crypto project's database stays outside, reached at `host.docker.internal`.

## Consequences

- Misses are visible without blocking anything, but there's no alerting yet.
- SLO targets are repeated on the dashboard, so a change means editing two places.
- `dagster dev` is fine for one machine, not for production.
