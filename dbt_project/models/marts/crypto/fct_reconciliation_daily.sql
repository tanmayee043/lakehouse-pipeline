{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='trade_date',
        on_schema_change='fail'
    )
}}
-- Per symbol and UTC day: did Project 1's live capture agree with the exchange's record?
--
-- Project 1 runs in sessions, often with hours switched off in between. A new capture session
-- starts when it captured nothing for more than `capture_session_break_minutes`. Missing trades
-- inside a session were lost while running (that's what completeness_pct measures); missing
-- trades between sessions happened while it was switched off. Both are filled from the record.
with compared as (
    select * from {{ ref('int_trades_compared') }}
),

captured as (
    select
        symbol,
        trade_date,
        trade_id,
        trade_time_utc,
        trade_time_utc - lag(trade_time_utc) over (partition by symbol, trade_date order by trade_id)
            > interval '{{ var("capture_session_break_minutes") }} minutes' as starts_session
    from compared
    where in_stream
),

sessions as (
    select symbol, trade_date, min(trade_id) as first_id, max(trade_id) as last_id
    from (
        select
            *,
            sum(case when starts_session then 1 else 0 end)
                over (partition by symbol, trade_date order by trade_id) as session_number
        from captured
    )
    group by symbol, trade_date, session_number
),

session_summary as (
    select
        symbol,
        trade_date,
        count(*) as capture_sessions,
        min(first_id) as first_captured_id,
        max(last_id) as last_captured_id
    from sessions
    group by all
),

official_classified as (
    select
        c.symbol,
        c.trade_date,
        c.trade_id,
        c.in_stream,
        c.trade_id between w.first_captured_id and w.last_captured_id as in_window,
        exists (
            select 1 from sessions as s
            where s.symbol = c.symbol and s.trade_date = c.trade_date and c.trade_id between s.first_id and s.last_id
        ) as in_session
    from compared as c
    left join session_summary as w using (symbol, trade_date)
    where c.in_official
),

official_counts as (
    select
        symbol,
        trade_date,
        count(*) as official_trades,
        count(*) filter (where in_stream) as matched_trades,
        count(*) filter (where in_window) as official_in_window,
        count(*) filter (where in_session) as official_in_sessions,
        count(*) filter (where in_session and not in_stream) as missed_while_running,
        count(*) filter (where in_window and not in_session) as missed_during_downtime
    from official_classified
    group by all
),

stream_counts as (
    select
        symbol,
        trade_date,
        count(*) filter (where in_stream) as captured_trades,
        count(*) filter (where in_stream and not in_official) as stream_only_trades,
        count(*) filter (where in_official and in_stream and official_price <> stream_price) as price_mismatches,
        count(*) filter (where in_official and in_stream and official_quantity <> stream_quantity)
            as quantity_mismatches
    from compared
    group by all
),

extracted as (
    select trade_date from {{ ref('stg_stream_extracts') }}
    where {{ crypto_window('trade_date') }}
),

daily as (
    select
        sc.symbol,
        sc.trade_date,
        coalesce(oc.official_trades, 0) as official_trades,
        sc.captured_trades,
        coalesce(oc.matched_trades, 0) as matched_trades,
        coalesce(oc.official_in_window, 0) as official_in_window,
        coalesce(oc.official_in_sessions, 0) as official_in_sessions,
        coalesce(oc.missed_while_running, 0) as missed_while_running,
        coalesce(oc.missed_during_downtime, 0) as missed_during_downtime,
        sc.stream_only_trades,
        sc.price_mismatches,
        sc.quantity_mismatches,
        coalesce(ss.capture_sessions, 0) as capture_sessions,
        ss.first_captured_id,
        ss.last_captured_id
    from stream_counts as sc
    left join official_counts as oc using (symbol, trade_date)
    left join session_summary as ss using (symbol, trade_date)
)

select
    d.symbol,
    d.trade_date,
    d.official_trades,
    d.captured_trades,
    d.matched_trades,
    d.official_in_window,
    d.official_in_window - d.matched_trades as missed_in_window,
    d.official_in_sessions,
    d.missed_while_running,
    d.missed_during_downtime,
    d.capture_sessions,
    d.stream_only_trades,
    d.price_mismatches,
    d.quantity_mismatches,
    d.first_captured_id,
    d.last_captured_id,
    case when d.official_in_sessions > 0
        then round(100.0 * (d.official_in_sessions - d.missed_while_running) / d.official_in_sessions, 4)
    end as completeness_pct,
    case when d.official_trades > 0
        then round(100.0 * d.matched_trades / d.official_trades, 4) end as day_coverage_pct,
    e.trade_date is not null as stream_extracted,
    case
        when d.official_trades = 0 then 'awaiting_official_record'
        when e.trade_date is null then 'stream_not_extracted'
        when d.captured_trades = 0 then 'not_captured'
        when d.stream_only_trades > 0 or d.price_mismatches > 0 or d.quantity_mismatches > 0 then 'disagrees'
        when d.missed_while_running > 0 then 'gaps_filled'
        when d.missed_during_downtime > 0 then 'downtime_filled'
        else 'complete'
    end as status
from daily as d
left join extracted as e using (trade_date)
