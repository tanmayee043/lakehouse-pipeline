# 9. Reconcile by trade ID, inside the Crypto project's capture sessions

**Status:** accepted (revised after the first live run)

## Context

Binance.US trade IDs are sequential per symbol, so the official record for a day is complete by construction and every trade has a stable key. The Crypto project only captures while it's running, and in practice it runs in short sessions with hours switched off in between.

The first version measured completeness over the Crypto project's first-to-last captured trade of each day, the same rule as its own gap checker. On real data that read 11.6%: on 30 Oct, the Crypto project ran around 03:36 and again around 22:03 UTC, and the 19 hours in between counted as "missed". It had actually captured every trade while it was running.

## Decision

Match the two sides on `(symbol, trade_id)` with a full outer join, per UTC day, and split the Crypto project's captured trades into **capture sessions**: a new session starts after more than `capture_session_break_minutes` (dbt variable, default 5) with nothing captured. Then:

- `missed_while_running`: official trades inside a session that the Crypto project doesn't have. A real capture failure; `completeness_pct` is measured on this.
- `missed_during_downtime`: official trades between sessions, while the Crypto project was switched off.
- `day_coverage_pct`: captured over the whole day, which mostly reflects uptime.

Both kinds of missing trade are filled from the official record. Price and quantity are compared as exact decimals. Each symbol-day gets one status: complete, gaps_filled, downtime_filled, disagrees, not_captured, stream_not_extracted or awaiting_official_record.

## Consequences

- On the real data so far: 100% completeness while running, 0 disagreements across 2,945 captured trades, and 22,423 downtime trades filled.
- The break threshold is a heuristic. A trade lost during a quiet market stretch of over 5 minutes would count as downtime instead of a loss. The variable makes this explicit and tunable.
- The Crypto project's own gap checker still reports the downtime as gaps; the two views measure different things and both are shown.
- A test fails the build if the official record ever has a break in its ID sequence, since every number here depends on that assumption.
