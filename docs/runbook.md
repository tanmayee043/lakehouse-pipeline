# Runbook

What to do when something goes wrong. Commands run from the repo root in PowerShell with the venv active (`venv\Scripts\Activate.ps1`). If Windows says "running scripts is disabled", run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.

## Where to look

| Question | Look here |
|---|---|
| Did the last run work? | Dagster → **Runs** (`$env:DAGSTER_HOME = "$PWD\dagster_home"; dagster dev`, then http://localhost:3000) |
| Which test failed? | Dagster → the asset's **Checks** tab |
| Does the stream agree with the exchange? | Dashboard → Crypto → Reconciliation |
| Is the data fresh, and are the SLOs met? | Dashboard → Crypto → Reconciliation, and Weather → Pipeline health ([slo.md](slo.md)) |

## Re-run or backfill

Dagster → **Jobs** → `daily_crypto_pipeline` → **Materialize**, then pick a day or a range (a range runs as one run). For the weather starter pipeline, use `daily_weather_pipeline` → **Materialize all**. Re-runs are always safe: each day is replaced, never duplicated. Only one run goes at a time, so others wait in the queue.

## Data is stale

The daily schedules ship switched off, and they only fire while `dagster dev` is running. Run the job now (above), then switch the schedule on under **Automation**.

## Warehouse is locked

`Could not set lock on file`, or "The warehouse is busy" on the dashboard. DuckDB allows one writer, so something else has it open: a running Dagster job, a `dbt build` in another terminal, or a notebook. Wait for the run or close the other process. For your own queries, open it read-only: `duckdb.connect("data/warehouse/lakehouse.duckdb", read_only=True)`.

## Crypto project isn't reachable

The run still succeeds, but reconciliation shows those days as "⏳ Not copied yet". Start the Crypto project's database (`docker compose up -d postgres` in its folder), check the `CRYPTO_PROJECT_POSTGRES_*` settings in `.env`, and re-run the days. It keeps 90 days of trades, so there's no rush.

## A test or contract failed

- **Data test:** run `dbt test --select <model> --store-failures --profiles-dir .` in `dbt_project/` to save the bad rows to a table.
- **Contract error:** a column changed name or type upstream. Update the model and its contract together.
- **`assert_reported_gaps_are_filled`:** the exchange record covers a gap but the trades aren't there, which means a download skipped trades. Re-run that day.

## What a reconciliation status means

| Status | Meaning | Do anything? |
|---|---|---|
| ✅ Complete | Every trade captured, prices and quantities match | No |
| 🩹 Gaps filled | The stream lost trades while running; they're filled from the official record | Check the Crypto project's logs (often a restart) |
| 🔌 Downtime filled | The stream was off for part of the day; those trades are filled | Only if it should have been running |
| ⚠️ Disagrees | A captured price or quantity differs from the exchange | Compare the day's files in `data/raw_crypto/stream_trades/` and `exchange_trades/`. `fct_trades` keeps the official value meanwhile |
| ⏸️ Not captured | The stream wasn't running that day | No |
| ⏳ Not copied yet | The Crypto project's database was unreachable | See above |
| ⏳ Awaiting official record | The stream has trades for a day not fetched yet | Run that day |

A gap marked **Partly filled** crosses midnight into a day that hasn't run yet. Run the next day.

## Query an old Iceberg snapshot

The dashboard's **Time travel** tab does this. From Python:

```python
from serving import iceberg
iceberg.snapshots("fct_reconciliation_daily")          # versions, newest first
iceberg.read("fct_reconciliation_daily", snapshot_id)  # the table as it was then
```

`new_snapshot: false` after a run just means nothing changed. To start the history over, delete `data/iceberg/` and run any crypto day.

## Docker Compose

- Logs: `docker compose logs --tail 50 dagster` (or `dashboard`)
- Stop: `docker compose down` (data and run history stay on disk)
- The containers reach the Crypto project at `host.docker.internal`, so only the port, database, user and password come from `.env`.
- Don't run `dagster dev` locally and in Compose at the same time: they share the same warehouse file.

## Weather fetch failed

You'll see "N of 79 cities failed to fetch". That's on purpose: dbt won't build on a partial day. Check [open-meteo.com](https://open-meteo.com) for an outage (`429` means rate limit, `5xx` means their side), wait, and re-run. A missed day can't be backfilled, because the API only serves forecasts from today on.

## Rebuild from scratch

The raw files are the source of truth, so the warehouse can always be thrown away:

```powershell
Remove-Item data\warehouse\lakehouse.duckdb
cd dbt_project; dbt build --profiles-dir .
```
