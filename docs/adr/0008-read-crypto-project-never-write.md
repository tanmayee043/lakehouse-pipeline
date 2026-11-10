# 8. Read the Crypto project, never write to it

**Status:** accepted

## Context

The lakehouse recovers trades the Crypto project missed. They could be written back into its database, or stay in the lakehouse.

## Decision

The lakehouse only reads. The connection is opened `READ ONLY`, a test proves writes are refused, and recovered trades live only in `fct_trades` (with fill status in `fct_trade_gaps`).

## Consequences

- A clean one-way boundary, and no write credentials needed.
- The Crypto project's dashboard still shows its gaps as open, which is honest: it did miss them live. The lakehouse shows them as filled.
- Write-back can be added later as one upsert step with its own credentials.
