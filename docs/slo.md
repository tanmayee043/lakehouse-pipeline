# Service level objectives

Four promises the pipeline makes: three for crypto, one for the weather starter pipeline. Each one is a dbt test that runs on every build and shows on the dashboard. A miss is a warning, not a failure: the usual causes (a schedule switched off, the Crypto project offline) aren't fixed by stopping the pipeline.

| SLO | Target | Measured by | If it's missed |
|---|---|---|---|
| **Crypto freshness** | Yesterday (UTC) reconciled by 03:00 UTC | Newest day in `fct_reconciliation_daily` | Run yesterday's partition and switch the schedule on ([runbook](runbook.md#re-run-or-backfill)) |
| **Crypto completeness** | ≥ 99.9% of trades captured while the stream was running (last 30 days) | Captured ÷ official trades inside capture sessions | Nothing to fix here, the trades are already filled. Check the Crypto project's logs |
| **Crypto agreement** | 0 captured trades that disagree with the exchange (last 30 days) | Price or quantity mismatches, plus trades the exchange doesn't have | [Runbook: Disagrees](runbook.md#what-a-reconciliation-status-means) |
| **Weather freshness** | Newest forecast under 26 hours old | Newest fetch time in `dim_city` | [Runbook: Data is stale](runbook.md#data-is-stale) |

## Why these targets

- **03:00 UTC** gives the 02:15 run 45 minutes for a job that takes 2–4, so one retry still makes it.
- **99.9% while running** matches the Crypto project's own target. Downtime is reported separately, not counted as a miss ([ADR 9](adr/0009-reconcile-by-trade-id-in-the-capture-window.md)).
- **0 disagreements**, because a trade ID either matches the exchange or it's a bug.
- **26 hours** is one daily run plus some slack.

Targets live in the `slo_*` variables in `dbt_project/dbt_project.yml` and are repeated at the top of each dashboard page, so change both.

## Status

All four are met on the real data as of 10 Nov. On the CI sample data the two freshness checks always warn, because the sample's dates are fixed in the past. That's expected.
