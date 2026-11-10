{% docs __overview__ %}
# Lakehouse Pipeline: data catalog

Checks whether a live crypto stream missed anything. The [Crypto project](https://github.com/tanmayee043/crypto-streaming-platform) captures Binance.US trades live; this pipeline compares them, trade by trade, with the exchange's official record and fills whatever was missed. The weather models (79 cities) are the starter pipeline I built the patterns on before integrating the crypto trades.

This site rebuilds on every merge from the repo's sample data, so row counts are the sample's, not real ones.

**How to read it:** open a model and click the graph icon (bottom right) for its lineage. Each mart page lists its enforced columns (the contract) and the tests that run on every build.

## Layers

| Layer | Crypto | Weather |
|---|---|---|
| Raw (sources) | `crypto_raw.*`: Parquet per UTC day, money as `DECIMAL(20,8)` | `raw.*`: JSON per run, plus the city list |
| Staging | `stg_exchange_trades`, `stg_stream_trades`, `stg_trade_gaps`, `stg_stream_extracts` (inlined, not stored) | `stg_cities`, `stg_weather_hourly` |
| Intermediate | `int_trades_compared`: official record vs live capture | `int_weather_latest`: newest forecast per city and hour |
| Marts | `fct_trades`, `fct_reconciliation_daily`, `fct_ohlcv_daily`, `fct_anomalies_daily`, `fct_trade_gaps` | `dim_city`, `fct_weather_hourly`, `fct_weather_daily` |

Start with **`fct_reconciliation_daily`**, the headline table.

The four daily crypto marts are also published to **Apache Iceberg** for time travel. That happens in Dagster, after dbt, so it's not in this graph.

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
