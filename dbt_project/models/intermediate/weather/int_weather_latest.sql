-- Forecasts from consecutive runs overlap by six days. For every city and hour,
-- keep only the value from the most recent run.
select *
from {{ ref('stg_weather_hourly') }}
qualify row_number() over (
    partition by city_id, forecast_hour_utc
    order by fetched_at_utc desc
) = 1
