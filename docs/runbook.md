# Runbook

How to operate the pipeline on a laptop. All commands run from the repo root in PowerShell, with the venv active (`venv\Scripts\Activate.ps1`).

## See what's going on

| Question | Where to look |
|---|---|
| Did today's run work? | Dagster UI → **Runs** (`$env:DAGSTER_HOME = "$PWD\dagster_home"; dagster dev`, then http://localhost:3000) |
| Is the data fresh? | Dashboard → **Pipeline health**, or `cd dbt_project; dbt source freshness --profiles-dir .` (warns after 26 h, errors after 50 h) |
| Which test failed? | Dagster UI → the asset's **Checks** tab, or `dbt build --profiles-dir .` output |
| Does the Crypto project agree with the exchange? | Dashboard → **Crypto** → Reconciliation |
| Are the SLOs met? | Dashboard → Crypto → Reconciliation (SLO row) and Weather → Pipeline health; `dbt test --select tag:slo --profiles-dir .`; what each one means: [slo.md](slo.md) |
| Is every city covered? | Dashboard → **Pipeline health** → "Cities with forecasts" (should be 79 / 79) |
| What did ingestion log? | Dagster run logs, or run it by hand: `python ingestion/fetch_weather.py --log-format json` |

## 1. Ingestion failed (API outage or rate limit)

**Symptoms:** the `raw/weather_forecasts` step is red in Dagster with "N of 79 cities failed to fetch", or the script exits with code 1 and logs `FAILED <city>`.

**Why it matters:** the step fails on purpose so dbt never builds on a partial day. Cities that did succeed are already saved for today.

**Fix:**
1. Check https://open-meteo.com for an outage notice, and look at the error in the log: `429` is a rate limit, `5xx` is their side.
2. Requests are already retried 4 times and the step twice. Wait, then re-run: Dagster → **Jobs → daily_weather_pipeline → Materialize all**, or `python ingestion/fetch_weather.py`.
3. Re-running the same day is safe: it overwrites today's files instead of duplicating them.

**Can't fix:** a missed day can't be backfilled. The API only serves forecasts from today onward, and `--date` only names the output folder. The marts still cover that day from the previous run's forecast.

## 2. Warehouse is locked

**Symptoms:** dbt fails with `Could not set lock on file`, or the dashboard shows "The warehouse is busy".

**Why:** DuckDB allows one writer. Something else has the file open for writing: a Dagster run in progress, a `dbt build` in another terminal, or a Python shell or notebook with an open connection.

**Fix:**
1. If a Dagster run is in progress, wait for it to finish. The dashboard recovers on its own.
2. Otherwise close the other process (the notebook kernel, the second terminal) and re-run.
3. For ad-hoc queries, always open the file read-only: `duckdb.connect("data/warehouse/lakehouse.duckdb", read_only=True)`.

## 3. Data is stale

**Symptoms:** the dashboard's **Pipeline health** says Stale or Out of date, `dbt source freshness` warns or errors, or the city page warns that the forecast doesn't cover the current hour.

**Why:** the daily job didn't run. The schedule ships switched off, and it only fires while `dagster dev` is running.

**Fix:**
1. Start Dagster and run the job now (see "See what's going on").
2. To keep it running daily, switch on `daily_weather_pipeline_schedule` under **Automation** and leave `dagster dev` running.

## A test or contract failed

- **Range or uniqueness test:** open the failing check in Dagster, or run `dbt test --select <model> --store-failures --profiles-dir .` to save the offending rows to a table. Bad API values usually show up first in `stg_weather_hourly`.
- **`assert_every_city_has_forecast`:** a city in `cities.csv` has no data. Either it failed every fetch, or its name was edited so it no longer matches its file name.
- **Contract error on a mart:** a column was renamed or changed type upstream. Update the model or the contract in `_marts.yml`, deliberately, together.

## Crypto project database isn't reachable

**Symptoms:** the `crypto_project_stream` step logs "the Crypto project's database isn't reachable", its metadata shows `crypto_project_reachable: false`, and the dashboard's Crypto → Reconciliation shows those days as "⏳ Not copied yet".

**Why it doesn't fail the run:** the official record and the analytics still build without the Crypto project. Only the comparison waits.

**Fix:**
1. Start the Crypto project's database: `docker start postgres` (in the Crypto project's folder: `docker compose up -d postgres`).
2. Check the lakehouse's `.env` has the `CRYPTO_PROJECT_POSTGRES_*` settings (see README).
3. Re-run the affected days (next section). The Crypto project keeps 90 days of trades, so there's time.

## Crypto: re-run or backfill days

Dagster UI → **Jobs → daily_crypto_pipeline → Materialize** → pick one partition (UTC day) or a range. A range runs as one run. Reruns are safe: each day is replaced, never duplicated. From the command line:

```powershell
$env:DAGSTER_HOME = "$PWD\dagster_home"
dagster job launch -j daily_crypto_pipeline --tags '{"dagster/partition": "2026-10-30"}'
```

Only one run executes at a time (DuckDB has one writer); the others wait in the queue.

## Crypto: what a reconciliation status means

| Status | Meaning | Action |
|---|---|---|
| ✅ Complete | Every official trade in the Crypto project's capture window was captured, with matching price and quantity | None |
| 🩹 Gaps filled | The Crypto project lost trades while it was running (inside a capture session); they're filled from the official record | None for the lakehouse. Look at the Crypto project's logs for why it missed them (often a restart) |
| 🔌 Downtime filled | The Crypto project was switched off for part of the day (no capture for over 5 minutes); those trades are filled from the official record, and completeness while running is unaffected | None, unless the Crypto project was supposed to be running |
| ⚠️ Disagrees | A captured price or quantity differs from the record, or the Crypto project has a trade the exchange doesn't | Compare that day's raw files for the trade: `data/raw_crypto/stream_trades/` (the Crypto project's copy) against `exchange_trades/` (official), both Parquet. `fct_trades` always keeps the official value, so analytics stay correct meanwhile |
| ⏸️ Not captured | The Crypto project wasn't running that day | None |
| ⏳ Not copied yet | The Crypto project's database wasn't reachable when the day ran | See "Crypto project database isn't reachable" |
| ⏳ Awaiting official record | The Crypto project has trades for a day the exchange history hasn't been fetched for | Run that day's partition |

**A gap shows "Partly filled":** it spans midnight into a day that hasn't run yet. Run the next day's partition. The `assert_reported_gaps_are_filled` test only fails once the record covers both sides of a gap and it's still not filled, which would mean a download skipped trades.

## Crypto: the gap sensor

`crypto_project_gap_sensor` (Automation tab, off by default) checks the Crypto project's open gaps every 15 minutes and re-runs each affected day once. A new gap on an already-run day triggers one more run of that day; an unchanged day is never re-run twice.

## Crypto: query an Iceberg snapshot

The four daily crypto marts are published to Iceberg in `data/iceberg/` after every crypto run. The dashboard's Crypto → **Time travel** tab shows any snapshot. From Python:

```python
from serving import iceberg

iceberg.snapshots("fct_reconciliation_daily")  # versions, newest first
iceberg.read("fct_reconciliation_daily", snapshot_id)  # the table as it was then (pyarrow)
```

From DuckDB (any engine that reads Iceberg works the same way):

```sql
install iceberg; load iceberg;
select * from iceberg_scan('<path printed by iceberg.metadata_location("fct_reconciliation_daily")>');
```

**A publish shows `new_snapshot: false`:** the rows were identical to the current snapshot, so nothing was added. That's expected on reruns.

**Start Iceberg history over:** delete `data/iceberg/` and run any crypto partition; the next publish recreates the tables from the warehouse. Older snapshots are lost.

## Running with Docker Compose

| Task | Command |
|---|---|
| Start (rebuilds after code changes) | `docker compose up --build -d` |
| See logs | `docker compose logs --tail 50 dagster` (or `dashboard`) |
| Run a command inside | `docker compose exec dagster dagster job launch -j daily_crypto_pipeline --tags '{"dagster/partition": "2026-11-06"}'` |
| Stop | `docker compose down` (data and run history stay in `./data` and `./dagster_home`) |

**The Crypto project isn't reachable from the container:** its Postgres must be running on this machine and listening on the port in `.env`. The containers reach it at `host.docker.internal`; Compose sets that host automatically, so only port, database, user and password come from `.env`.

**Don't run `dagster dev` locally and in Compose at the same time:** both use `./dagster_home` and the same DuckDB file, and only one may write.

## Rebuild everything from the raw files

The raw JSON is the source of truth, so the warehouse can always be thrown away:

```powershell
Remove-Item data\warehouse\lakehouse.duckdb
cd dbt_project
dbt build --profiles-dir .
```
