{% docs __overview__ %}
# Lakehouse Pipeline: data catalog

A batch pipeline that answers one question for crypto trades: **do the live stream and the official history agree?** The [Crypto project](https://github.com/tanmayee043/crypto-streaming-platform) captures Binance.US trades live; this pipeline downloads the exchange's official record of every trade, reconciles the two trade by trade, and fills whatever the live capture missed. A second, smaller dataset holds 7-day hourly weather forecasts for 79 cities.

This site is generated from the dbt project on every merge to `main`, built on the repository's small sample data, so row counts here are the sample's, not production's.

## How to read it

- **Lineage:** open any model and click the graph icon (bottom right) to see where its data comes from and what uses it. Raw sources are green; models are blue.
- **Contracts:** every mart lists its columns with enforced types and not-null constraints. dbt refuses to build a mart whose shape drifts.
- **Tests:** each model's page lists the data tests that run on every build.

## Layers

| Layer | Crypto | Weather |
|---|---|---|
| Raw (sources) | `crypto_raw.*`: Parquet per UTC day, money as `DECIMAL(20,8)` | `raw.*`: JSON per run, plus the city list |
| Staging | `stg_exchange_trades`, `stg_stream_trades`, `stg_trade_gaps`, `stg_stream_extracts` (inlined, not stored) | `stg_cities`, `stg_weather_hourly` |
| Intermediate | `int_trades_compared`: official record vs live capture | `int_weather_latest`: newest forecast per city and hour |
| Marts | `fct_trades`, `fct_reconciliation_daily`, `fct_ohlcv_daily`, `fct_anomalies_daily`, `fct_trade_gaps` | `dim_city`, `fct_weather_hourly`, `fct_weather_daily` |

Start with **`fct_reconciliation_daily`**: it's the headline table.

After each run, the four daily crypto marts are also published as **Apache Iceberg** tables, with a snapshot per change for time travel and readable by any engine. That step runs outside dbt (in Dagster), so it isn't in this lineage graph.

## More

- [Repository and README](https://github.com/tanmayee043/lakehouse-pipeline)
- [Service level objectives](https://github.com/tanmayee043/lakehouse-pipeline/blob/main/docs/slo.md)
- [Architecture decision records](https://github.com/tanmayee043/lakehouse-pipeline/tree/main/docs/adr)
{% enddocs %}

{% docs col_symbol %}
Trading pair as the exchange names it, for example `BTCUSDT` (uppercase).
{% enddocs %}

{% docs col_trade_id %}
The exchange's trade ID. Sequential per symbol, so a day's official record is complete when every ID from its first to its last is present.
{% enddocs %}

{% docs col_trade_date %}
UTC calendar day of the trade. Crypto data is partitioned and reconciled by this day.
{% enddocs %}

{% docs col_city_id %}
ASCII slug of the city name (`Montréal` → `montreal`), matching the raw file name. The join key across the weather models.
{% enddocs %}
