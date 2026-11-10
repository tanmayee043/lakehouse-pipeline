{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='trade_date',
        on_schema_change='fail'
    )
}}
-- Daily candles per symbol from the complete official record (exact decimals throughout).
-- Incremental by day like fct_trades: a run recomputes only the days it covers.
select
    symbol,
    trade_date,
    arg_min(price, trade_id) as open_price,
    max(price) as high_price,
    min(price) as low_price,
    arg_max(price, trade_id) as close_price,
    cast(sum(quantity) as decimal(38, 8)) as volume,
    cast(sum(quote_amount) as decimal(38, 8)) as quote_volume,
    cast(sum(quote_amount) / sum(quantity) as decimal(20, 8)) as vwap,
    count(*) as trade_count
from {{ ref('fct_trades') }}
where {{ crypto_window('trade_date') }}
group by all
