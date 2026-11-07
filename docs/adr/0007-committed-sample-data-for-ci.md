# 7. Committed sample data for CI

**Status:** accepted

## Context

CI has to run `dbt build` and the integration tests on every push. Calling the live API from CI would make results depend on the weather and on Open-Meteo being up, and live data changes every day.

## Decision

Commit a small sample in `tests/data/` (57 KB): 3 cities chosen for their edge cases (London; Montréal for the accented name; Sydney for the southern hemisphere at UTC+11), each with two overlapping runs. The older run starts a day earlier and is 1 °C warmer, so any leak from it into the overlap is easy to detect. dbt reads it through the `LAKEHOUSE_DATA_DIR` variable.

## Consequences

- CI is offline and deterministic, and tests build throwaway warehouses without touching `data/`.
- The sample can drift from the real API's shape. If Open-Meteo changes its response, CI won't notice; `dbt build` on the next real run will.

## Update: crypto sample

`tests/data/raw_crypto/` holds the Crypto project's committed crash-day recording (10,507 real trades) as the official record, plus a copy as the Crypto project would store it with known faults planted: a 10-trade and a 1-trade gap, one price off by 0.01, and one anomaly of each type. Tests assert that reconciliation finds exactly those. `tests/data/crypto_project/schema.sql` is a copy of the Crypto project's schema, loaded into a throwaway Postgres (Testcontainers) to test the read-only extract. See `tests/data/README.md`.
