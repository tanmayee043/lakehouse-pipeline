# 12. SLOs warn instead of failing; Docker Compose for local runs

**Status:** accepted

## Context

The portfolio standards ask for written SLOs checked automatically, and a one-command setup with pinned versions. Two questions came up: what should happen when an SLO is missed, and how much of a production layout belongs in a local setup.

## Decision

**SLOs are dbt tests with `severity: warn`, tagged `slo`** (`dbt_project/tests/slo/`), with targets in dbt variables. A miss shows as a warning in `dbt build`, as a warning check in Dagster, and as "Missed" on the dashboard, and the pipeline keeps running. The common causes of a miss are a schedule switched off or the Crypto project offline; stopping the build fixes neither and would also hold back data that is fine.

**Docker Compose runs one image as two services:** Dagster (`dagster dev`: webserver and daemon in one process) and the Streamlit dashboard. Both mount `./data` and `./dagster_home`, so data and run history survive restarts and match a non-Docker run. The Crypto project's database stays outside, reached at `host.docker.internal`. The base image is pinned (`python:3.11.14-slim`, the same as the Crypto project) and Python packages are pinned in `requirements.txt`. Ports are bound to localhost only.

## Consequences

- Breaches are visible in three places without blocking anything. Someone still has to look: there's no alerting yet.
- The dashboard repeats the SLO numbers as constants, so a target change means editing two places (noted in docs/slo.md).
- `dagster dev` is fine on one machine but not a production deployment. A server deployment would split the webserver and daemon and keep run history in Postgres instead of SQLite files.
- Tested: the stack starts, both UIs load, the Dagster container reached the Crypto project's database, and a crypto partition run inside the container succeeded (5 Nov → 6 Nov reconciled, all four SLOs met).
