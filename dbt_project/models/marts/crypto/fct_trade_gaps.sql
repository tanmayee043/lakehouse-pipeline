-- The Crypto project's reported gaps, and whether the lakehouse has filled each one from the
-- exchange's record. The lakehouse never writes back, so the Crypto project still lists them as open.
--
-- A gap can span midnight, so it may be only partly filled until the next day's record is in.
-- Once the record has the trades on both sides of the gap, sequential IDs mean every missing
-- trade must be there too: that's when the gap is "covered" and must be filled.
with gaps as (
    select * from {{ ref('stg_trade_gaps') }}
),

trades as (
    select symbol, trade_id, trade_date from {{ ref('fct_trades') }}
),

filled as (
    select
        g.symbol,
        g.first_missing_id,
        count(t.trade_id) as filled_count,
        min(t.trade_date) as trade_date
    from gaps as g
    left join trades as t
        on t.symbol = g.symbol and t.trade_id between g.first_missing_id and g.last_missing_id
    group by all
),

covered as (
    select
        g.symbol,
        g.first_missing_id,
        exists (
            select 1 from trades as t where t.symbol = g.symbol and t.trade_id = g.first_missing_id - 1
        ) and exists (
            select 1 from trades as t where t.symbol = g.symbol and t.trade_id = g.last_missing_id + 1
        ) as official_record_covers_gap
    from gaps as g
)

select
    g.symbol,
    g.first_missing_id,
    g.last_missing_id,
    g.missing_count,
    f.trade_date,
    f.filled_count,
    c.official_record_covers_gap,
    f.filled_count = g.missing_count as filled_by_lakehouse,
    case
        when f.filled_count = g.missing_count then 'filled'
        when f.filled_count > 0 then 'partly_filled'
        else 'awaiting_official_record'
    end as fill_status,
    g.detected_at_utc,
    g.resolved_in_crypto_project_at_utc
from gaps as g
join filled as f using (symbol, first_missing_id)
join covered as c using (symbol, first_missing_id)
