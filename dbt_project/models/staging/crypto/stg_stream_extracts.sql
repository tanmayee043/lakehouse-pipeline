select distinct
    cast(regexp_extract(file, 'trade_date=(\d{4}-\d{2}-\d{2})', 1) as date) as trade_date
from {{ source('crypto_raw', 'stream_extracts') }}
