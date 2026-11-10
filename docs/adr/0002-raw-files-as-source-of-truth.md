# 2. Raw files as the source of truth

**Status:** accepted

## Context

API responses are the only copy of the data, and a past day's forecast can't be fetched again. Models and bugs will change over time.

## Decision

Save every API response untouched to `data/raw_json/fetched_on=YYYY-MM-DD/<city>.json` and never edit it. Every table is rebuilt from these files. Writes go through a temp file and a rename, so a crash never leaves half a file.

## Consequences

- A bug fix applies to all history with one rebuild.
- Re-running a day overwrites its files instead of duplicating them.
- The folder grows about 1.3 MB a day and is never pruned, which is fine for years at this size.
