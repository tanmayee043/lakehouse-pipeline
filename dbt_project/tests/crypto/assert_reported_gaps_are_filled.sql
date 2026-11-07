{{ config(meta={'dagster': {'ref': {'name': 'fct_trade_gaps'}}}) }}

-- Once the official record holds the trades on both sides of a gap the Crypto project reported, every
-- trade inside it must be in fct_trades. Gaps whose days aren't all fetched yet (for example
-- one spanning midnight into a day not run yet) are pending, not failures.
select *
from {{ ref('fct_trade_gaps') }}
where official_record_covers_gap and not filled_by_lakehouse
