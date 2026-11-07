# 8. Read Project 1, never write to it

**Status:** accepted

## Context

Project 1 (the crypto streaming platform) stores live trades and the gaps its gap checker finds in its own Postgres. The lakehouse can recover missed trades from the exchange's official record. Those trades could go back into Project 1's `trades` table, so its gap checker would mark the gaps resolved, or stay in the lakehouse.

## Decision

The lakehouse only reads Project 1. Its connection is opened `READ ONLY`, a test proves writes are refused, and recovered trades live only in the lakehouse's own tables (`fct_trades`, with the fill status in `fct_trade_gaps`).

## Consequences

- One-way boundary: Project 1 produces, the lakehouse consumes. The lakehouse needs no write credentials, and Project 1 never gets rows its detector didn't score.
- Project 1's own dashboard keeps showing the gaps as open, which is accurate: it really did miss them live. The lakehouse's dashboard shows them as filled.
- If write-back is wanted later, it's one extra step that upserts from `fct_trades` into Project 1 with its own credentials.
