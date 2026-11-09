# Service level objectives

What the pipeline promises, how each promise is measured, and where it's checked. A missed SLO is reported as a **warning** in dbt and Dagster and on the dashboard; it never stops the pipeline, because the usual causes (the schedule being off, the Crypto project being switched off) aren't fixed by stopping.

| SLO | Target | Measured as (SLI) | Window | Checked by | Shown on |
|---|---|---|---|---|---|
| **Crypto freshness** | Yesterday (UTC) is reconciled by **03:00 UTC** | Newest `trade_date` in `fct_reconciliation_daily` | Each check | dbt test `slo_crypto_reconciled_through_yesterday`; `dbt source freshness` on the exchange record (warns at 36 h, errors at 60 h) | Crypto → Reconciliation → SLO row |
| **Crypto completeness** | **≥ 99.9%** of official trades captured while the Crypto project was running | `completeness_pct` per symbol and day: captured ÷ official trades inside its capture sessions | Last 30 days | dbt test `slo_crypto_completeness_while_running` | Crypto → Reconciliation → tiles and SLO row |
| **Crypto agreement** | **0** captured trades that disagree with the official record | Price or quantity mismatches plus captured trades the exchange has no record of | Last 30 days | dbt test `slo_crypto_no_disagreements` | Crypto → Reconciliation → tiles and SLO row |
| **Weather freshness** | Newest forecast fetched within **26 hours** | Newest `last_fetched_at_utc` in `dim_city` | Each check | dbt test `slo_weather_forecasts_fresh`; `dbt source freshness` (warns at 26 h, errors at 50 h) | Weather → Pipeline health |

The targets live in one place for the pipeline, the `slo_*` variables in `dbt_project/dbt_project.yml`, and are repeated as constants at the top of each dashboard page.

## Why these targets

- **03:00 UTC** gives the 02:15 UTC crypto schedule 45 minutes for a day that normally takes 2–4 minutes, so one retry still meets it.
- **99.9% while running** matches the Crypto project's own completeness SLO. Measuring inside capture sessions keeps it about capture quality: the Crypto project being switched off is reported as downtime, not as a miss ([ADR 9](adr/0009-reconcile-by-trade-id-in-the-capture-window.md)).
- **0 disagreements**, because a trade ID either has the exchange's price and quantity or it doesn't. Any disagreement is a bug somewhere.
- **26 hours** is a daily run plus slack for a late start.

## When one is missed

| SLO | Most likely cause | What to do |
|---|---|---|
| Crypto freshness | The crypto schedule is off, `dagster dev` isn't running, or the day's run failed | Run yesterday's partition ([runbook](runbook.md#crypto-re-run-or-backfill-days)); switch the schedule on |
| Crypto completeness | The Crypto project lost trades while running (often around a restart) | Nothing to fix here, the trades are already filled. Look at the Crypto project's logs for that time |
| Crypto agreement | A bug in the Crypto project's storage, or a download problem here | [Runbook: Disagrees](runbook.md#crypto-what-a-reconciliation-status-means) |
| Weather freshness | The weather schedule is off or the run failed | [Runbook: Data is stale](runbook.md#3-data-is-stale) |

## Current status

All four are met on the real data as of 7 Nov: crypto reconciled through 6 Nov, 100% completeness while running, 0 disagreements, and a weather fetch within the last 26 hours. Crypto freshness only holds while the daily schedule is on: before 6 Nov was run, it was missed (newest day 5 Nov), which is how the check was first seen working.

On the CI sample data (`tests/data/`) the two freshness SLOs always warn, because its dates are deliberately fixed in the past. That's expected: warnings don't fail the build.
