# 11. Parquet raw layer with exact decimals

**Status:** accepted

## Context

Trades are money, so they must never be floats. There are also far more of them than weather rows (about 49,000 a day).

## Decision

Save crypto data as Parquet, one folder per UTC day, with prices and quantities as `DECIMAL(20,8)`, the same type the Crypto project uses. Responses are checked with Pydantic, and values stay `Decimal` all the way to the marts. Empty days still get an empty file, so "nothing captured" differs from "never fetched".

## Consequences

- dbt only reads the days a run needs, and money stays exact (a test recomputes every daily candle in Python).
- No time travel on raw files; the published marts have it instead ([ADR 13](0013-iceberg-for-the-published-marts.md)).
