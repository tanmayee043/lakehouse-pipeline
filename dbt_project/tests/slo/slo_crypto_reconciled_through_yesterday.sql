{{ config(meta={'dagster': {'ref': {'name': 'fct_reconciliation_daily'}}}) }}

-- SLO: from 03:00 UTC, yesterday (UTC) has been reconciled. Returns a row when it hasn't.
with checked as (
    select
        now() at time zone 'UTC' as checked_at_utc,
        (select max(trade_date) from {{ ref('fct_reconciliation_daily') }}) as newest_day
)

select *
from checked
where hour(checked_at_utc) >= {{ var('slo_crypto_reconciled_by_utc_hour') }}
    and (newest_day is null or newest_day < cast(checked_at_utc as date) - 1)
