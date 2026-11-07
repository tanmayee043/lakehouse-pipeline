# 11. Parquet raw layer with exact decimals

**Status:** accepted (Iceberg planned on top)

## Context

Trades carry money, which must never be a float. The weather source stores raw JSON, but trades are far more numerous (about 49,000 a day for 4 symbols) and strongly typed.

## Decision

Land crypto data as Parquet, one folder per UTC day (`trade_date=YYYY-MM-DD`), with prices and quantities as `DECIMAL(20,8)`, the same type Project 1 uses. API responses are validated with Pydantic, and values stay `Decimal` from the JSON response to the marts. Empty days still get an (empty) file, so "nothing captured" is distinguishable from "never extracted".

## Consequences

- dbt reads only the day folders a run needs, and money stays exact end to end (a test recomputes every daily candle in Python `Decimal` and compares).
- Plain Parquet has no schema evolution or time travel. Apache Iceberg can be added over these same files later without changing the models.
