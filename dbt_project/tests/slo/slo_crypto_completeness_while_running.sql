{{ config(meta={'dagster': {'ref': {'name': 'fct_reconciliation_daily'}}}) }}

-- SLO: inside the Crypto project's capture sessions, at least 99.9% of official trades were
-- captured, for every symbol and day in the last 30 days. Returns the days that missed it.
select symbol, trade_date, completeness_pct, missed_while_running
from {{ ref('fct_reconciliation_daily') }}
where completeness_pct < {{ var('slo_crypto_completeness_pct') }}
    and trade_date >= cast(now() at time zone 'UTC' as date) - {{ var('slo_window_days') }}
