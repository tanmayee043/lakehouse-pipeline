# 3. Local time plus IANA time zone from the API

**Status:** accepted (replaced an earlier GMT-only request)

## Context

Daily summaries should follow each city's own calendar: Tokyo's Tuesday isn't the GMT Tuesday. The first version asked Open-Meteo for GMT times, which gives no way of knowing a city's time zone. A fixed UTC offset would also be wrong on daylight-saving change days.

## Decision

Request `timezone=auto`. Open-Meteo then returns local wall-clock times plus the IANA zone name (for example `Asia/Tokyo`). Staging keeps the local time and derives UTC with DuckDB's time-zone conversion, which handles DST. Daily summaries group by local date and expect 23 or 25 hours on DST change days.

## Consequences

- Models have both clocks: UTC for ordering and dedupe, local time for days and charts.
- The test suite checks offsets for London (0), Montréal (−5) and Sydney (+11).
- Relies on DuckDB's ICU time-zone data being current.
