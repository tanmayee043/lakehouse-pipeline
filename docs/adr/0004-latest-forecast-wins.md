# 4. Latest forecast wins, in its own model

**Status:** accepted

## Context

Each daily run fetches 7 days ahead, so consecutive runs overlap by six days. Without a rule, every hour would appear up to seven times.

## Decision

`int_weather_latest` keeps, for each city and UTC hour, the value from the run with the newest `fetched_at`. The rule lives in one intermediate model that all marts read from, and a uniqueness test on `(city_id, forecast_hour_utc)` guards it.

## Consequences

- The marts always show the most recent forecast; older forecasts stay in staging and in the raw files.
- Hours that only an older run covers (days already in the past) are kept, so history accumulates.
- Comparing how forecasts changed between runs (forecast accuracy) would need its own model over staging, not the marts.
- Tested with sample data where the older run is 1 °C warmer: no older value appears in the overlap.
