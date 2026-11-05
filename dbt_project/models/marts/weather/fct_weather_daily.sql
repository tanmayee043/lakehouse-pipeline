-- Daily summary per city, using each city's local calendar day.
with hourly as (
    select
        city_id,
        timezone,
        cast(forecast_hour_local as date) as forecast_date_local,
        temperature_c,
        precipitation_mm,
        wind_speed_kmh
    from {{ ref('int_weather_latest') }}
),

daily as (
    select
        city_id,
        forecast_date_local,
        any_value(timezone) as timezone,
        min(temperature_c) as temperature_min_c,
        max(temperature_c) as temperature_max_c,
        round(avg(temperature_c), 1) as temperature_avg_c,
        round(sum(precipitation_mm), 1) as precipitation_total_mm,
        count(*) filter (where precipitation_mm > 0) as rainy_hours,
        max(wind_speed_kmh) as wind_speed_max_kmh,
        count(*) as hours_observed
    from hourly
    group by city_id, forecast_date_local
)

select
    city_id,
    forecast_date_local,
    temperature_min_c,
    temperature_max_c,
    temperature_avg_c,
    precipitation_total_mm,
    rainy_hours,
    wind_speed_max_kmh,
    hours_observed,
    -- A local day is 23 or 25 hours long when DST changes, so compute it instead of assuming 24.
    hours_observed = date_diff(
        'hour',
        timezone(timezone, cast(forecast_date_local as timestamp)),
        timezone(timezone, cast(forecast_date_local + interval 1 day as timestamp))
    ) as is_complete_day
from daily
