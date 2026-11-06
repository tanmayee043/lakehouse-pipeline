# 2. Raw files as the source of truth

**Status:** accepted

## Context

API responses are the only copy of the data, and forecasts can't be fetched again later: once a day has passed, that day's forecast is gone. Models, tests and bugs will change over time.

## Decision

Ingestion writes each API response untouched (plus the city's metadata and fetch time) to `data/raw_json/fetched_on=YYYY-MM-DD/<city>.json`. Nothing downstream edits these files. Every table is rebuilt from them by `dbt build`. Writes go through a temp file and a rename, so a crash never leaves half-written JSON behind.

## Consequences

- Any model change or bug fix applies to all history with one rebuild.
- Re-running a day overwrites that day's files instead of adding duplicates.
- The raw folder grows by about 1.3 MB a day and is never pruned. That's fine for years at this size, but retention or compaction will be needed eventually.
- The raw layer is plain files, not a table format: no schema evolution or time travel (Iceberg is on the roadmap).
