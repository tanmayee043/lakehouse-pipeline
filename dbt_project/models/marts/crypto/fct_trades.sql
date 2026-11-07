{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='trade_date',
        on_schema_change='fail'
    )
}}
-- Every official trade exactly once, gaps in the live capture filled from the exchange's
-- record, with Project 1's live anomaly scores where it captured the trade.
-- Incremental by day: a run replaces whole days, so rerunning a day changes nothing.
select
    symbol,
    trade_id,
    trade_date,
    trade_time_utc,
    official_price as price,
    official_quantity as quantity,
    cast(official_price * official_quantity as decimal(38, 8)) as quote_amount,
    is_buyer_maker,
    in_stream as captured_live,
    is_anomaly,
    anomaly_type,
    price_z,
    size_ratio
from {{ ref('int_trades_compared') }}
where in_official
