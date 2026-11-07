{{ config(meta={'dagster': {'ref': {'name': 'fct_trades'}}}) }}

-- Binance.US trade IDs are sequential per symbol, so each day's official record must be one
-- unbroken ID range. A break means a page was skipped during the download.
select symbol, trade_date, count(*) as trades, max(trade_id) - min(trade_id) + 1 as id_range
from {{ ref('fct_trades') }}
group by all
having count(*) <> max(trade_id) - min(trade_id) + 1
