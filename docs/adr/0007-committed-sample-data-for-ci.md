# 7. Committed sample data for CI

**Status:** accepted

## Context

CI runs `dbt build` and the tests on every push. Calling live APIs would make results change daily and depend on someone else's uptime.

## Decision

Commit a small sample in `tests/data/`, picked for edge cases:

- **Weather:** London, Montréal (accented name) and Sydney (UTC+11), each with two overlapping runs, the older one 1 °C warmer.
- **Crypto:** 10,507 real trades as the official record, plus a copy with planted faults (lost trades, an outage, a wrong price).
- **Crypto project schema:** loaded into a throwaway Postgres with Testcontainers.

Details are in [tests/data/README.md](../../tests/data/README.md).

## Consequences

- CI is offline and repeatable, and tests check that exactly the planted faults are found.
- If an API changes its response shape, CI won't notice; the next real run will.
