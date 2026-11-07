select
    upper(symbol) as symbol,
    trade_id,
    cast(trade_date as date) as trade_date,
    trade_time at time zone 'UTC' as trade_time_utc,
    cast(price as decimal(20, 8)) as price,
    cast(quantity as decimal(20, 8)) as quantity,
    is_buyer_maker
from {{ source('crypto_raw', 'exchange_trades') }}
