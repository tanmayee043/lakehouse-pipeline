{{ config(meta={'dagster': {'ref': {'name': 'fct_reconciliation_daily'}}}) }}

-- SLO: no captured trade disagrees with the official record (price, quantity, or a trade the
-- exchange doesn't have) in the last 30 days. Returns the days that had any.
select symbol, trade_date, price_mismatches, quantity_mismatches, stream_only_trades
from {{ ref('fct_reconciliation_daily') }}
where price_mismatches + quantity_mismatches + stream_only_trades > 0
    and trade_date >= cast(now() at time zone 'UTC' as date) - {{ var('slo_window_days') }}
