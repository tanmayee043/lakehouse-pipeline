# Test sample data

Small, committed inputs so CI never calls a live API or database. `LAKEHOUSE_DATA_DIR` points dbt here.

## Weather (`raw_csv/`, `raw_json/`)

3 cities picked for edge cases: London, Montréal (accented name) and Sydney (southern hemisphere, UTC+11). Each has two runs. The run fetched on 2026-11-04 starts a day earlier and is 1 °C warmer than the 2026-11-05 run, so any older value that leaks into the overlap is easy to spot.

## Crypto (`raw_crypto/`)

Built from Project 1's committed recording of 10 Oct 2025 19:00 – 11 Oct 01:00 UTC (10,507 real Binance.US trades for BTCUSDT, ETHUSDT and SOLUSDT, no ID gaps).

- `exchange_trades/`: the recording as-is, standing in for the exchange's official record.
- `stream_trades/`: the same trades as Project 1 would store them, with known faults planted:
  - BTCUSDT, 10 Oct: trades 30826284–30826293 (10) missing
  - ETHUSDT, 10 Oct: trade 18001585 missing
  - SOLUSDT, 10 Oct: a 10-minute outage, 21:00–21:10 UTC (trades 11404488–11404665, 178 trades)
  - SOLUSDT, 11 Oct: trade 11407327 stored 0.01 too high
  - one anomaly of each type (price, volume, both), and the first 100 trades per symbol unscored (detector warm-up)
  - lowercase symbols, to exercise the staging guard
- `trade_gaps/`: Project 1's gap list naming all three gaps (BTC, ETH, SOL outage), all open.

## Project 1 schema (`project1/schema.sql`)

A copy of Project 1's database schema. The Testcontainers tests load it into a throwaway Postgres to test the read-only extract against the real table shapes.
