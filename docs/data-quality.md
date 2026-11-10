# Data quality

What the pipeline guarantees, how, and how I know it works. The [README](../README.md#data-quality) has the short version.

## Crypto

| Guarantee | How | Proof |
|---|---|---|
| The official record is complete | Trade IDs are sequential, so a test fails on any break in a day's range | XRPUSDT on 5 Nov: 8,663 trades = last ID − first ID + 1 |
| Every trade appears once | `fct_trades` is unique on symbol + trade ID | Sample: 10,507 trades in, 10,507 out |
| Reconciliation finds real faults, and only those | Full outer join on trade ID; exact decimal comparison | Sample has planted losses, an outage and a price off by 0.01; tests find exactly those |
| Downtime isn't counted as loss | Completeness counts only inside capture sessions (5-minute break) | The first live run said 11.6% because the stream had been off for hours. Now 100% ([ADR 9](adr/0009-reconcile-by-trade-id-in-the-capture-window.md)) |
| Reported gaps get filled | A test fails if a gap is still open once the record covers it | Real data: 8 of 8 gaps filled (22,423 trades), one across midnight |
| Money is never a float | `DECIMAL(20,8)` from the API to the marts | A test recomputes every daily candle in Python `Decimal` |
| Re-runs change nothing | Incremental models replace whole days | Running a day twice leaves every mart's checksum the same |
| A run only touches its own days | The date window filters both reads and writes | Rebuilding one day leaves another day's rows in place |
| Backfills are routine | A range runs as one Dagster run | Real 14-day backfill: one run, 12.8 minutes, all tests passed |
| The Crypto project is never changed | Connection opened `READ ONLY` | Against a real Postgres (Testcontainers), a `DELETE` is refused |
| Any engine reads the same data | Marts published as Iceberg | DuckDB and PyIceberg return identical rows for all four tables |
| History only grows on real change | Unchanged publishes are skipped; snapshots expire after 90 days | Tests cover skip, time travel, expiry and a new column |

## Weather (starter pipeline)

I built these patterns on weather first, before integrating the crypto trades.

| Guarantee | How | Proof |
|---|---|---|
| No builds on partial data | Ingestion fails if any city fails, so dbt doesn't run | A bad URL leaves no partial files |
| Responses have the right shape | Pydantic checks every response before saving | Bad timezone, arrays, nulls and timestamps are each rejected |
| Re-runs change nothing | Same-day runs overwrite the same files | Two runs give the same checksums; a 0.1 °C change is caught |
| No duplicate hours | The newest forecast wins per city and hour | The older sample run is 1 °C warmer and never leaks through |
| Time zones are right | Local time plus IANA zone, converted with DST | London 0, Montréal −5, Sydney +11 |

## Performance

On a laptop (i5-1135G7, 12 GB RAM): one crypto day takes 2–4 minutes, a 14-day backfill took 12.8, and a weather run about 2½. The full test suite (45 tests plus 6 browser tests) takes about 3–4 minutes.
