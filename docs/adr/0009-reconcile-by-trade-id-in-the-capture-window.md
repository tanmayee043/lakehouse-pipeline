# 9. Reconcile by trade ID, inside capture sessions

**Status:** accepted (revised after the first live run)

## Context

Binance.US trade IDs are sequential, so the official record is complete by construction. The Crypto project only captures while it's running, often in short sessions. My first version measured completeness from its first to last trade of the day and got 11.6%: the hours it was switched off counted as "missed", even though it hadn't lost a single trade while running.

## Decision

Match both sides on symbol + trade ID with a full outer join, and split captured trades into sessions (a new one starts after 5 minutes with nothing captured). Then:

- **Missed while running:** a real loss. Completeness is measured on this.
- **Missed during downtime:** the stream was off.

Both are filled from the official record, and prices and quantities are compared as exact decimals.

## Consequences

- Real data: 100% completeness while running, 0 disagreements, 22,423 downtime trades filled.
- The 5-minute break is a judgment call. A loss during a very quiet market stretch would count as downtime, so it's a dbt variable you can tune.
- A test fails if the official record ever has an ID gap, since everything depends on that.
