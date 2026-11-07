-- Project 1 stores the exchange's uppercase symbols; upper() guards the join against any hand-entered row.
select
    upper(symbol) as symbol,
    trade_id,
    cast(trade_date as date) as trade_date,
    trade_time at time zone 'UTC' as trade_time_utc,
    cast(price as decimal(20, 8)) as price,
    cast(quantity as decimal(20, 8)) as quantity,
    is_anomaly,
    anomaly_type,
    price_z,
    size_ratio,
    received_at at time zone 'UTC' as received_at_utc,
    stored_at at time zone 'UTC' as stored_at_utc
from {{ source('crypto_raw', 'stream_trades') }}
