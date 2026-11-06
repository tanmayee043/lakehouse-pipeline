# 7. Committed sample data for CI

**Status:** accepted

## Context

CI has to run `dbt build` and the integration tests on every push. Calling the live API from CI would make results depend on the weather and on Open-Meteo being up, and live data changes every day.

## Decision

Commit a small sample in `tests/data/` (57 KB): 3 cities chosen for their edge cases (London; Montréal for the accented name; Sydney for the southern hemisphere at UTC+11), each with two overlapping runs. The older run starts a day earlier and is 1 °C warmer, so any leak from it into the overlap is easy to detect. dbt reads it through the `LAKEHOUSE_DATA_DIR` variable.

## Consequences

- CI is offline and deterministic, and tests build throwaway warehouses without touching `data/`.
- The sample can drift from the real API's shape. If Open-Meteo changes its response, CI won't notice; `dbt build` on the next real run will.
