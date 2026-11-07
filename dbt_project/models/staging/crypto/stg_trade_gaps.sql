-- Only the newest snapshot: it is Project 1's current gap list.
with snapshots as (
    select *, cast(snapshot_date as date) as snapshot_day
    from {{ source('crypto_raw', 'trade_gaps') }}
)

select
    upper(symbol) as symbol,
    first_missing_id,
    last_missing_id,
    last_missing_id - first_missing_id + 1 as missing_count,
    detected_at at time zone 'UTC' as detected_at_utc,
    resolved_at at time zone 'UTC' as resolved_in_project1_at_utc
from snapshots
where snapshot_day = (select max(snapshot_day) from snapshots)
