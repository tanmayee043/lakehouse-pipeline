-- Every trade from either side, matched on (symbol, trade_id): the official record vs what
-- The Crypto project captured live. Trade IDs are sequential per symbol, so this join is exact.
with official as (
    select * from {{ ref('stg_exchange_trades') }}
    where {{ crypto_window('trade_date') }}
),

stream as (
    select * from {{ ref('stg_stream_trades') }}
    where {{ crypto_window('trade_date') }}
)

select
    coalesce(o.symbol, s.symbol) as symbol,
    coalesce(o.trade_id, s.trade_id) as trade_id,
    coalesce(o.trade_date, s.trade_date) as trade_date,
    coalesce(o.trade_time_utc, s.trade_time_utc) as trade_time_utc,
    o.trade_id is not null as in_official,
    s.trade_id is not null as in_stream,
    o.price as official_price,
    o.quantity as official_quantity,
    o.is_buyer_maker,
    s.price as stream_price,
    s.quantity as stream_quantity,
    s.is_anomaly,
    s.anomaly_type,
    s.price_z,
    s.size_ratio
from official as o
full outer join stream as s
    on o.symbol = s.symbol and o.trade_id = s.trade_id
