{{ config(meta={'dagster': {'ref': {'name': 'dim_city'}}}) }}

-- SLO: the newest forecast was fetched in the last 26 hours (a daily run plus slack).
with checked as (
    select
        now() at time zone 'UTC' as checked_at_utc,
        (select max(last_fetched_at_utc) from {{ ref('dim_city') }}) as last_fetched_at_utc
)

select *
from checked
where last_fetched_at_utc is null
    or last_fetched_at_utc < checked_at_utc - interval '{{ var("slo_weather_max_age_hours") }} hours'
