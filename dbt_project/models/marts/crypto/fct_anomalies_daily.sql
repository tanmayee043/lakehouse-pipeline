-- Project 1's live anomaly flags per symbol and day. Only trades it captured were scored.
select
    symbol,
    trade_date,
    count(*) filter (where captured_live) as captured_trades,
    count(*) filter (where captured_live and price_z is not null) as scored_trades,
    count(*) filter (where is_anomaly) as anomalies,
    count(*) filter (where anomaly_type = 'price') as price_anomalies,
    count(*) filter (where anomaly_type = 'volume') as volume_anomalies,
    count(*) filter (where anomaly_type = 'both') as both_anomalies,
    case when count(*) filter (where captured_live and price_z is not null) > 0
        then round(100.0 * count(*) filter (where is_anomaly)
            / count(*) filter (where captured_live and price_z is not null), 4) end as anomaly_rate_pct
from {{ ref('fct_trades') }}
group by all
having count(*) filter (where captured_live) > 0
