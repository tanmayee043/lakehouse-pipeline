# 4. Latest forecast wins, in its own model

**Status:** accepted

## Context

Each daily run fetches 7 days ahead, so runs overlap by six days. Without a rule, each hour would appear up to seven times.

## Decision

`int_weather_latest` keeps the newest run's value for each city and hour. Every mart reads from it, and a uniqueness test guards it.

## Consequences

- The marts show the latest forecast; older ones stay in staging and the raw files.
- Comparing forecasts across runs (accuracy) would need its own model.
- Tested with an older sample run that's 1 °C warmer: it never leaks into the overlap.
