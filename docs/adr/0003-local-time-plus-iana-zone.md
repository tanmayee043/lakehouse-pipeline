# 3. Local time plus IANA time zone

**Status:** accepted (replaced an earlier GMT-only request)

## Context

Daily summaries should follow each city's own calendar: Tokyo's Tuesday isn't London's. Asking the API for GMT loses the time zone, and a fixed offset is wrong on daylight-saving days.

## Decision

Request `timezone=auto`, which returns local times plus the zone name (like `Asia/Tokyo`). Staging keeps local time and converts to UTC with DuckDB, which handles DST. Days with 23 or 25 hours are expected.

## Consequences

- Models have both clocks: UTC for ordering, local time for days and charts.
- Tests check London (0), Montréal (−5) and Sydney (+11).
